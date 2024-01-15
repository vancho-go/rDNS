package storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/vancho-go/rDNS/internal/app/dnslookuper"
	"github.com/vancho-go/rDNS/internal/app/models"
	"golang.org/x/sync/semaphore"
	"log"
	"sync"
	"sync/atomic"
)

type Storage struct {
	DB *sql.DB
}

func Initialize(uri string) (*Storage, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("initialize: error opening database: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("initialize: error verifing database connection: %w", err)
	}

	err = createIfNotExists(db)
	if err != nil {
		return nil, fmt.Errorf("initialize: error creating database structure: %w", err)
	}
	return &Storage{DB: db}, nil
}

func createIfNotExists(db *sql.DB) error {
	createTablesQuery := `
		CREATE TABLE IF NOT EXISTS ip_addresses (
			ip_address INET NOT NULL UNIQUE PRIMARY KEY
		);
		CREATE TABLE IF NOT EXISTS fqdns (
			id SERIAL PRIMARY KEY,
			fqdn VARCHAR(255) NOT NULL,
			ip_address INET DEFAULT NULL REFERENCES ip_addresses(ip_address) ON DELETE CASCADE,
			ttl TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
			outdated BOOLEAN DEFAULT FALSE NOT NULL,
			UNIQUE(fqdn, ip_address)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_fqdns_non_null_ip ON fqdns(fqdn, ip_address, outdated) WHERE ip_address IS NOT NULL;
		CREATE UNIQUE INDEX IF NOT EXISTS idx_fqdns_null_ip ON fqdns(fqdn, outdated) WHERE ip_address IS NULL;
		CREATE INDEX IF NOT EXISTS ix_fqdns_outdated_ttl ON fqdns(ttl,outdated);
`

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("createIfNotExists: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(createTablesQuery)
	if err != nil {
		return fmt.Errorf("createIfNotExists: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("createIfNotExists: %w", err)
	}
	return nil
}

func (s *Storage) UploadFQDN(ctx context.Context, fqdns []models.APIUploadFQDNRequest) error {

	conn, err := stdlib.AcquireConn(s.DB)
	if err != nil {
		return fmt.Errorf("uploadFQDN: error acquiring conn: %w", err)
	}
	defer stdlib.ReleaseConn(s.DB, conn)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("uploadFQDN: error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "CREATE TEMP TABLE temp_fqdns (fqdn text) ON COMMIT DROP")
	if err != nil {
		return fmt.Errorf("uploadFQDN: error creating temp table: %w", err)
	}

	_, err = conn.CopyFrom(
		ctx,
		pgx.Identifier{"temp_fqdns"},
		[]string{"fqdn"},
		pgx.CopyFromRows(fqdnsToPgxRows(fqdns)),
	)
	if err != nil {
		return fmt.Errorf("uploadFQDN: error during COPY to temp table: %w", err)
	}

	_, err = tx.Exec(ctx, `
       INSERT INTO fqdns (fqdn)
       SELECT fqdn FROM temp_fqdns
       ON CONFLICT (fqdn, outdated) WHERE ip_address IS NULL DO NOTHING
   `)
	if err != nil {
		return fmt.Errorf("uploadFQDN: error inserting data from temp table: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("uploadFQDN: error committing transaction: %w", err)
	}

	return nil
}

func fqdnsToPgxRows(requests []models.APIUploadFQDNRequest) [][]interface{} {
	rows := make([][]interface{}, 0, len(requests))
	for _, req := range requests {
		rows = append(rows, []interface{}{req.FQDN})
	}
	return rows
}

func (s *Storage) UpdateDNSRecords(ctx context.Context, fqdnProcessed *int64) {
	//	здесь будут запускаться задачи

	// Определяем максимальное количество одновременных операций резолвинга и записи в базу данных
	maxConcurrentResolves := 4000
	maxConcurrentWrites := 2000

	// Cемафоры для резолва и обновления
	resolveSem := semaphore.NewWeighted(int64(maxConcurrentResolves))
	writeSem := semaphore.NewWeighted(int64(maxConcurrentWrites))

	select {
	case <-ctx.Done():
		fmt.Println("updateDNSRecords: update task cancelled by context")
	default:
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		outputFQDNsChannel, err := s.getOutdatedDNSRecords(ctx)
		if err != nil {
			log.Println(err)
			return
		}

		var stageUpdateFQDNChannels []<-chan string
		var errors []<-chan error

		for i := 0; i < 4000; i++ {
			updateFQDNChannel, updateFQDNErrors, err := s.prepareAndUpdateDNSRecordBatch(ctx, outputFQDNsChannel, resolveSem, writeSem)
			if err != nil {
				log.Println(err)
				return
			}
			stageUpdateFQDNChannels = append(stageUpdateFQDNChannels, updateFQDNChannel)
			errors = append(errors, updateFQDNErrors)
		}
		stageUpdateFQDNMerged := mergeChannels(ctx, stageUpdateFQDNChannels...)
		errorsMerged := mergeChannels(ctx, errors...)

		s.fqdnConsumer(ctx, cancel, stageUpdateFQDNMerged, errorsMerged, fqdnProcessed)
	}
}

func (s *Storage) getOutdatedDNSRecords(ctx context.Context) (<-chan string, error) {
	// producer

	outputChannel := make(chan string)

	rows, err := s.DB.Query("SELECT DISTINCT fqdn FROM fqdns WHERE outdated=false and(ttl<NOW() or ttl is null or ttl>=NOW() ) LIMIT 100000")
	if err != nil {
		return nil, fmt.Errorf("getOutdatedDNSRecords: error selecting fqdns: %w", err)
	}

	go func() {
		defer close(outputChannel)
		defer rows.Close()

		for rows.Next() {

			var fqdn string
			if err := rows.Scan(&fqdn); err != nil {
				log.Println(fmt.Errorf("getOutdatedDNSRecords: error coping fqdn to var: %w", err).Error())
				continue
			}

			select {
			case <-ctx.Done():
				return
			case outputChannel <- fqdn:
			}

		}
		if err := rows.Err(); err != nil {
			log.Println(fmt.Errorf("getOutdatedDNSRecords: error after iterating rows: %w", err).Error())
			return
		}
	}()

	return outputChannel, nil
}

func (s *Storage) prepareAndUpdateDNSRecordBatch(ctx context.Context, fqdns <-chan string, resolveSem, writeSem *semaphore.Weighted) (<-chan string, <-chan error, error) {
	outChannel := make(chan string)
	errorChannel := make(chan error)
	batchSize := 1500

	resolvedBatchCh := make(chan map[string][]models.ResolverResponse)

	// Горутина для резолва
	go func() {
		defer close(resolvedBatchCh)
		var batch []string
		for fqdn := range fqdns {
			batch = append(batch, fqdn)
			if len(batch) >= batchSize {
				if err := resolveSem.Acquire(ctx, 1); err != nil {
					errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error acquiring resolve semaphore: %v", err)
					continue
				}

				resolverResponses, err := dnslookuper.ResolveDNSWithTTLBatch(batch, errorChannel)
				resolveSem.Release(1)
				if err != nil {
					errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error during resolving batch: %w", err)
					return

				} else {
					resolvedBatchCh <- resolverResponses
				}
				batch = batch[:0] // очистка пакета для следующего использования
			}
		}
		// Обрабатываем оставшиеся записи, если они остались
		if len(batch) > 0 {
			if err := resolveSem.Acquire(ctx, 1); err != nil {
				errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error acquiring resolve semaphore: %v", err)
				return
			}

			resolverResponses, err := dnslookuper.ResolveDNSWithTTLBatch(batch, errorChannel)
			resolveSem.Release(1)
			if err != nil {
				errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error during resolving batch: %w", err)
				return

			} else {
				resolvedBatchCh <- resolverResponses
			}
		}
	}()

	// Горутина для записи
	go func() {
		defer close(outChannel)
		defer close(errorChannel)
		for resolverResponses := range resolvedBatchCh {
			if err := writeSem.Acquire(ctx, 1); err != nil {
				errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error acquiring write semaphore: %v", err)
				continue
			}

			var fqdns []string
			for fqdn := range resolverResponses {
				fqdns = append(fqdns, fqdn)
			}

			if err := s.updateDNSRecordsBatch(ctx, fqdns, resolverResponses); err != nil {
				errorChannel <- err
			} else {
				for _, fqdn := range fqdns {
					outChannel <- fmt.Sprintf("FQDN '%s' updated", fqdn)
				}
			}
			writeSem.Release(1)
		}
	}()

	return outChannel, errorChannel, nil
}

func mergeChannels[T any](ctx context.Context, ce ...<-chan T) <-chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	output := func(c <-chan T) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(len(ce))
	for _, c := range ce {
		go output(c)

	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (s *Storage) updateDNSRecordsBatch(ctx context.Context, fqdns []string, resolverResponses map[string][]models.ResolverResponse) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("updateDNSRecordsBatch: error beginning transaction: %w", err)
	}
	defer tx.Rollback()

	for _, fqdn := range fqdns {
		_, err = tx.Exec("UPDATE fqdns SET outdated = TRUE WHERE fqdn = $1", fqdn)
		if err != nil {
			return fmt.Errorf("updateDNSRecordsBatch: error updating outdated fields: %w", err)
		}

		for _, rr := range resolverResponses[fqdn] {
			_, err = tx.Exec(`
				INSERT INTO ip_addresses (ip_address) VALUES ($1)
				ON CONFLICT (ip_address) DO NOTHING;`, rr.IPAddress)
			if err != nil {
				return fmt.Errorf("updateDNSRecordsBatch: error inserting ip addresses: %w", err)
			}

			_, err = tx.Exec(`
				INSERT INTO fqdns (fqdn, ip_address, ttl, outdated)
				VALUES ($1, $2, $3, FALSE)
				ON CONFLICT (fqdn, ip_address)
				DO UPDATE SET ttl = EXCLUDED.ttl, outdated = FALSE
				WHERE fqdns.fqdn = EXCLUDED.fqdn AND fqdns.ip_address = EXCLUDED.ip_address;`,
				fqdn, rr.IPAddress, rr.ExpiresAt)
			if err != nil {
				return fmt.Errorf("updateDNSRecordsBatch: error inserting into fqdns: %w", err)
			}
		}

		_, err = tx.Exec("DELETE FROM fqdns WHERE fqdn = $1 AND outdated = TRUE", fqdn)
		if err != nil {
			return fmt.Errorf("updateDNSRecordsBatch: error deleting outdated fqdns: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("updateDNSRecordsBatch: error committing transaction: %w", err)
	}

	return nil
}

func (s *Storage) fqdnConsumer(ctx context.Context, cancel context.CancelFunc, fqdns <-chan string, errors <-chan error, fqdnProcessed *int64) {
	// consumer
	for {
		select {
		case <-ctx.Done():
			log.Println(ctx.Err().Error())
			return

		case err, ok := <-errors:
			if ok {
				//cancel()
				atomic.AddInt64(fqdnProcessed, 1)
				_ = err
				log.Println(err.Error())
			}

		case fqdn, ok := <-fqdns:
			if ok {
				atomic.AddInt64(fqdnProcessed, 1)
				_ = fqdn
				log.Println(fqdn)
			} else {
				return
			}
		}
	}
}

func (s *Storage) GetFQDNs(ctx context.Context, ipAddresses models.APIGetFQDNsRequest) (models.APIGetFQDNsResponse, error) {
	result := models.APIGetFQDNsResponse{IPAddresses: make(map[string][]string, len(ipAddresses.IPAddresses))}
	for _, ip := range ipAddresses.IPAddresses {
		var fqdns []string
		query := `SELECT fqdn FROM fqdns WHERE ip_address = $1;`
		rows, err := s.DB.QueryContext(ctx, query, ip)
		if err != nil {
			return models.APIGetFQDNsResponse{}, fmt.Errorf("getFQDN: error getting fqdns: %w", err)
		}

		defer rows.Close()

		for rows.Next() {
			var fqdn string
			if err := rows.Scan(&fqdn); err != nil {
				return models.APIGetFQDNsResponse{}, fmt.Errorf("getFQDN: error scanning column: %w", err)
			}
			fqdns = append(fqdns, fqdn)
		}

		if err = rows.Err(); err != nil {
			return models.APIGetFQDNsResponse{}, fmt.Errorf("getFQDN: rows error: %w", err)
		}

		result.IPAddresses[ip] = fqdns
		if err := rows.Close(); err != nil {
			return models.APIGetFQDNsResponse{}, fmt.Errorf("getFQDN: rows close error: %w", err)
		}
	}
	return result, nil
}
