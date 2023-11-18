# Highload Microservices

## Preamble

I want to work with *Apache Kafka* and learn how to make communication between *microservices*.
In this project were implemented 2 microservices:
**1. HTTP server**
Responseble for get requests from clients and send responses to them
**2. Database writer**
Responseble for  write data about some patient into postgteSQL database.

Writing data into database implemented by importing from *csv file*, because many INSERT SQL query may reduce database perfomance.


## Sending data
```json
{
    "name":"John",
    "last_name":"Doe",
    "date_of_birth":"2000-01-01",
    "blood_type": 1,
    "rh_factor":"positive"
}
```

## Tests
Application were tested by *Apache Jmeter*. 
Maximum throughput is reached **74 482** rpm and used 1.8 GB RAM.

## Installation
Init git repository in your local host machine and execute commands below:
```bash
git clone https://github.com/KeninTown/MircoserviceHighLoad
cd MircoserviceHighLoad
make run
```
Make sure that *docker-daemon* is launched.