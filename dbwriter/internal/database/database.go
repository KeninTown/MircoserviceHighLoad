package database

import (
	"database/sql"
	"dbWriter/internal/config"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type Repository struct {
	Db *sql.DB
}

func Connect(cfg *config.Config) (*Repository, error) {
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

	return &Repository{Db: db}, nil
}

func (r *Repository) ImportFromCsv(fileName string) error {
	filePath := fmt.Sprintf("/csv/%s", fileName)
	log.Println("file path is ", filePath)

	query := fmt.Sprintf("COPY patients FROM '%s' DELIMITER ',' CSV HEADER;", filePath)
	fmt.Println("query = ", query)

	_, err := r.Db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to import data from csv file: %w", err)
	}
	return nil
}

func (r Repository) FindBiggestId() (int, error) {
	stmt, err := r.Db.Prepare("SELECT MAX(id) from patients")
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
