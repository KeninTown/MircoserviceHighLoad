package database

import (
	"database/sql"
	"dbWriter/internal/config"
	"dbWriter/internal/entities"
	"dbWriter/pkg/sl"
	"errors"
	"fmt"

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

	query := fmt.Sprintf("COPY patients FROM '%s' DELIMITER ',' CSV HEADER;", filePath)

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

func (r Repository) FindPatient(id int) (entities.Patient, error) {
	stmt, err := r.db.Prepare("SELECT * from patients where id = ?")
	if err != nil {
		return entities.Patient{}, fmt.Errorf("failed to prepare statement for finding maximum id in patients talbe: %w", err)
	}
	var patient entities.Patient
	err = stmt.QueryRow(id).Scan(&patient)
	if errors.Is(err, sql.ErrNoRows) {
		return entities.Patient{}, fmt.Errorf("patient is not exist")
	}

	return patient, nil
}

func (r Repository) Close() {
	if err := r.db.Close(); err != nil {
		slog.Error("failed to close connection with database", sl.Error(err))
	}
}
