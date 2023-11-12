package entities

type Patient struct {
	Id          uint   `json:"id"`
	Name        string `json:"name"`
	LastName    string `json:"last_name"`
	DateOfBirth string `json:"date_of_birth"`
	BloodType   uint8  `json:"blood_type"`
	RhFactor    string `json:"rh_factor"`
}
