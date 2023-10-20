package csvwriter

import (
	entities "dbWriter/internal/database/models"
	"fmt"
	"log"
	"os"
)

type CsvWriter struct {
	File *os.File
}

//TODO: implement create start file there

func New() *CsvWriter {
	return &CsvWriter{}
}

func (cw *CsvWriter) CreateNewFile(path string) {
	tempFile, err := os.CreateTemp(path, "patients-*.csv")
	err = os.Chmod(tempFile.Name(), 0644)
	if err != nil {
		log.Fatal("failed to change chmod")
	}
	log.Println("Chmo changes to 0644")
	if err != nil {
		log.Fatal("failed to create temp file: %w", err)
	}

	fmt.Println(tempFile)
	if cw.File != nil { //delete this if man
		cw.File.Close()
		os.Remove(cw.File.Name())
	}

	fmt.Fprintln(tempFile, "id,name,last_name,date_of_birth,blood_type,rh_factor")
	cw.File = tempFile
}

func (cw *CsvWriter) Write(patient entities.Patient, id int) error {
	_, err := fmt.Fprintf(cw.File, "%d,%s,%s,%s,%d,%s\n", id, patient.Name, patient.LastName,
		patient.DateOfBirth, patient.BloodType, patient.RhFactor)
	if err != nil {
		return fmt.Errorf("failed to write to csv file: %w", err)
	}

	return nil
}
