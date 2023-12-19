package storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/vancho-go/rDNS/internal/app/dnslookuper"
	"github.com/vancho-go/rDNS/internal/app/models"
	"log"
	"runtime"
	"sync"
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
		CREATE UNIQUE INDEX IF NOT EXISTS idx_fqdns_null_ip ON fqdns(fqdn, outdated) WHERE ip_address IS NULL;`

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

func (s *Storage) UpdateDNSRecords(ctx context.Context) {
	//	здесь будут запускаться задачи

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

		for i := 0; i < runtime.NumCPU(); i++ {
			updateFQDNChannel, updateFQDNErrors, err := s.prepareAndUpdateDNSRecord(ctx, outputFQDNsChannel)
			if err != nil {
				log.Println(err)
				return
			}
			stageUpdateFQDNChannels = append(stageUpdateFQDNChannels, updateFQDNChannel)
			errors = append(errors, updateFQDNErrors)
		}
		stageUpdateFQDNMerged := mergeChannels(ctx, stageUpdateFQDNChannels...)
		errorsMerged := mergeChannels(ctx, errors...)

		s.fqdnConsumer(ctx, cancel, stageUpdateFQDNMerged, errorsMerged)
	}
}

func (s *Storage) getOutdatedDNSRecords(ctx context.Context) (<-chan string, error) {
	// producer

	outputChannel := make(chan string)

	rows, err := s.DB.Query("SELECT DISTINCT fqdn FROM fqdns WHERE outdated=false and(ttl<NOW() or ttl is null )")
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

func (s *Storage) prepareAndUpdateDNSRecord(ctx context.Context, fqdns <-chan string) (<-chan string, <-chan error, error) {
	outChannel := make(chan string)
	errorChannel := make(chan error)

	go func() {
		defer close(outChannel)
		defer close(errorChannel)

		select {
		case <-ctx.Done():
			return
		case fqdn, ok := <-fqdns:
			if ok {
				err := s.updateDNSRecord(ctx, fqdn)
				if err != nil {
					errorChannel <- err
				} else {
					outChannel <- fmt.Sprintf("prepareAndUpdateDNSRecord: FQDN '%s' updated ", fqdn)
				}
			} else {
				return
			}

		}
	}()
	return outChannel, errorChannel, nil
}

func (s *Storage) updateDNSRecord(ctx context.Context, fqdn string) error {

	resolverResponse, err := dnslookuper.ResolveDNSWithTTL(fqdn)
	if err != nil {
		err = fmt.Errorf("updateDNSRecord: error resolving fqdn: %w", err)
		return err
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		err = fmt.Errorf("updateDNSRecord: error beginning transaction: %w", err)
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE fqdns SET outdated = TRUE WHERE fqdn = $1", fqdn)
	if err != nil {
		err = fmt.Errorf("updateDNSRecord: error updating outdated fields: %w", err)
		return err
	}

	for _, rr := range resolverResponse {
		ipAddress := rr.IPAddress
		expiresAt := rr.ExpiresAt

		_, err := tx.Exec(`INSERT INTO ip_addresses (ip_address) VALUES ($1)
			ON CONFLICT (ip_address) DO NOTHING;`, ipAddress)
		if err != nil {
			err = fmt.Errorf("updateDNSRecord: error inserting ip addresses: %w", err)
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO fqdns (fqdn, ip_address, ttl, outdated)
			VALUES ($1, $2, $3, FALSE)
			ON CONFLICT (fqdn, ip_address)
			DO UPDATE SET ttl = EXCLUDED.ttl, outdated = FALSE WHERE fqdns.fqdn = EXCLUDED.fqdn AND fqdns.ip_address = EXCLUDED.ip_address;`, fqdn, ipAddress, expiresAt)

		if err != nil {
			err = fmt.Errorf("updateDNSRecord: error inserting into fqdn: %w", err)
			return err
		}
	}
	_, err = tx.Exec("DELETE FROM fqdns WHERE fqdn = $1 AND ip_address IS NULL", fqdn)
	if err != nil {
		err = fmt.Errorf("updateDNSRecord: error deleting FQDNs with NULL ip: %w", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		err = fmt.Errorf("updateDNSRecord: error committing transaction: %w", err)
		return err
	}
	return nil
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

func (s *Storage) fqdnConsumer(ctx context.Context, cancel context.CancelFunc, fqdns <-chan string, errors <-chan error) {
	// consumer
	for {
		select {
		case <-ctx.Done():
			log.Println(ctx.Err().Error())
			return

		case err, ok := <-errors:
			if ok {
				cancel()
				log.Println(err.Error())
			}

		case fqdn, ok := <-fqdns:
			if ok {
				log.Println(fqdn)
			} else {
				return
			}
		}
	}
}

func (s *Storage) GetFQDNs(ctx context.Context, ipAddresses models.APIGetFQDNsRequest) (models.APIGetFQDNsResponse, error) {
	result := models.APIGetFQDNsResponse{make(map[string][]string, len(ipAddresses.IPAddresses))}
	for _, ip := range ipAddresses.IPAddresses {
		fqdns := []string{}
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
