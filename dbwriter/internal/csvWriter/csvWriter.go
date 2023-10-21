package csvwriter

import (
	entities "dbWriter/internal/entities"
	"dbWriter/pkg/sl"
	"fmt"
	"log"
	"os"

	"golang.org/x/exp/slog"
)

type CsvWriter struct {
	file *os.File
	Dir  string
}

//TODO: implement create start file there

func New(dirPath string) *CsvWriter {
	return &CsvWriter{Dir: dirPath}
}

func (cw *CsvWriter) CreateNewFile() {
	tempFile, err := os.CreateTemp(cw.Dir, "patients-*.csv")
	if err != nil {
		log.Fatal("failed to create temp file: %w", err)
	}

	err = os.Chmod(tempFile.Name(), 0644)
	if err != nil {
		log.Fatal("failed to change chmod")
	}

	if cw.file != nil {
		cw.file.Close()
		os.Remove(cw.GetPathToFile())
	}

	fmt.Fprintln(tempFile, "id,name,last_name,date_of_birth,blood_type,rh_factor")
	cw.file = tempFile
}

func (cw *CsvWriter) Write(patient entities.Patient, id int) error {
	_, err := fmt.Fprintf(cw.file, "%d,%s,%s,%s,%d,%s\n", id, patient.Name, patient.LastName,
		patient.DateOfBirth, patient.BloodType, patient.RhFactor)
	if err != nil {
		return fmt.Errorf("failed to write to csv file: %w", err)
	}

	return nil
}

func (cw CsvWriter) GetFileName() (string, error) {
	fileInfo, err := cw.file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get fileName, err: %s", err.Error())
	}
	return fileInfo.Name(), nil
}

func (cw CsvWriter) GetPathToFile() string {
	return cw.file.Name()
}

func (cw CsvWriter) Close() {
	if err := cw.file.Close(); err != nil {
		slog.Error("failed to close csv writer", sl.Error(err))
	}
}
