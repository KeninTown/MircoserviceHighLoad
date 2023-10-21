package database

import (
	"database/sql"
	"dbWriter/internal/config"
	"dbWriter/pkg/sl"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"golang.org/x/exp/slog"
)

type Repository struct {
	db *sql.DB
}

func Connect(cfg *config.DatabaseConfig) (*Repository, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=%s", cfg.User, cfg.Password, cfg.DBname, cfg.Host, cfg.Port, cfg.SSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgresql: %w", err)
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS patients(
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		last_name TEXT NOT NULL,
		date_of_birth TIMESTAMP NOT NULL,
		blood_type INTEGER NOT NULL,
		rh_factor TEXT NOT NULL);
		`)

	if err != nil {
		return nil, fmt.Errorf("failed to create patinet's table: %w", err)
	}

	return &Repository{db: db}, nil
}

func (r *Repository) ImportFromCsv(fileName string) error {
	filePath := fmt.Sprintf("/csv/%s", fileName)
	log.Println("file path is ", filePath)

	query := fmt.Sprintf("COPY patients FROM '%s' DELIMITER ',' CSV HEADER;", filePath)
	fmt.Println("query = ", query)

	_, err := r.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to import data from csv file: %w", err)
	}
	return nil
}

func (r Repository) FindBiggestId() (int, error) {
	stmt, err := r.db.Prepare("SELECT MAX(id) from patients")
	if err != nil {
		return 1, fmt.Errorf("failed to prepare statement for finding maximum id in patients talbe: %w", err)
	}

	var id int
	err = stmt.QueryRow().Scan(&id)
	if err != nil {
		return 1, fmt.Errorf("failed to find maximum id in patients table: %w", err)
	}

	return id + 1, nil
}

func (r Repository) Close() {
	if err := r.db.Close(); err != nil {
		slog.Error("failed to close connection with database", sl.Error(err))
	}
}
