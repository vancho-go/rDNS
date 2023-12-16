package storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/vancho-go/rDNS/internal/app/models"
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
			ttl INTEGER DEFAULT NULL,
			last_ip_address_update TIMESTAMP DEFAULT NULL,
			outdated BOOLEAN DEFAULT FALSE NOT NULL
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

//func (s *Storage) UploadFQDN(ctx context.Context, fqdns []models.APIUploadFQDNRequest) error {
//	tx, err := s.DB.BeginTx(ctx, nil)
//	if err != nil {
//		return fmt.Errorf("uploadFQDN: error starting transaction: %w", err)
//	}
//	defer tx.Rollback()
//
//	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("fqdns", "fqdn"))
//	if err != nil {
//		return fmt.Errorf("uploadFQDN: error preparing statement: %w", err)
//	}
//	defer stmt.Close()
//
//	for _, fqdn := range fqdns {
//		_, err = stmt.ExecContext(ctx, fqdn.FQDN)
//		if err != nil {
//			return fmt.Errorf("uploadFQDN: error inserting FQDN %s: %w", fqdn.FQDN, err)
//		}
//	}
//
//	_, err = stmt.ExecContext(ctx)
//	if err != nil {
//		return fmt.Errorf("uploadFQDN: %w", err)
//	}
//
//	if err := tx.Commit(); err != nil {
//		return fmt.Errorf("uploadFQDN: error committing transaction: %w", err)
//	}
//	return nil
//}

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
