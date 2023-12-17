package storage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/vancho-go/rDNS/internal/app/dnslookuper"
	"github.com/vancho-go/rDNS/internal/app/models"
	"log/slog"
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

func (s *Storage) UpdateDNSRecords() {
	rows, err := s.DB.Query("SELECT fqdn FROM fqdns WHERE outdated=false and(ttl<NOW() or ttl is null )")
	if err != nil {
		slog.Error(fmt.Errorf("updateDNSRecords: error selecting fqdns: %w", err).Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		var fqdn string
		if err := rows.Scan(&fqdn); err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error coping fqdn to var: %w", err).Error())
			continue
		}

		resolverResponse, err := dnslookuper.ResolveDNSWithTTL(fqdn)
		if err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error resolving fqdn: %w", err).Error())
			continue
		}

		tx, err := s.DB.Begin()
		if err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error beginning transaction: %w", err).Error())
			return
		}
		defer tx.Rollback()

		_, err = tx.Exec("UPDATE fqdns SET outdated = TRUE WHERE fqdn = $1", fqdn)
		if err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error updating outdated fields: %w", err).Error())
			return
		}

		for _, rr := range resolverResponse {
			ipAddress := rr.IPAddress
			expiresAt := rr.ExpiresAt

			_, err := tx.Exec(`INSERT INTO ip_addresses (ip_address) VALUES ($1)
			ON CONFLICT (ip_address) DO NOTHING;`, ipAddress)
			if err != nil {
				slog.Error(fmt.Errorf("updateDNSRecords: error inserting ip addresses: %w", err).Error())
				return
			}

			_, err = tx.Exec(`
			INSERT INTO fqdns (fqdn, ip_address, ttl, outdated)
			VALUES ($1, $2, $3, FALSE)
			ON CONFLICT (fqdn, ip_address) 
			DO UPDATE SET ttl = EXCLUDED.ttl, outdated = FALSE WHERE fqdns.fqdn = EXCLUDED.fqdn AND fqdns.ip_address = EXCLUDED.ip_address;`, fqdn, ipAddress, expiresAt)

			if err != nil {
				slog.Error(fmt.Errorf("updateDNSRecords: error inserting into fqdn: %w", err).Error())
				return
			}
		}

		_, err = tx.Exec("DELETE FROM fqdns WHERE fqdn = $1 AND ip_address IS NULL", fqdn)
		if err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error deleting FQDNs with NULL ip: %w", err).Error())
			return
		}

		err = tx.Commit()
		if err != nil {
			slog.Error(fmt.Errorf("updateDNSRecords: error committing transaction: %w", err).Error())
			return
		}
	}
}

func (s *Storage) GetFQDN(ctx context.Context, ipAddresses models.APIGetFQDNsRequest) (models.APIGetFQDNsResponse, error) {
	result := models.APIGetFQDNsResponse{make(map[string][]string, len(ipAddresses.IPAddresses))}
	for _, ip := range ipAddresses.IPAddresses {
		fqdns := []string{}
		query := `SELECT fqdn FROM fqdns WHERE ip_address = $1;`
		rows, err := s.DB.Query(query, ip)
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
