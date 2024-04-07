/*
1. install database driver: go get github.com/lib/pq
2. import mandatory:
"database/sql"
_ "github.com/lib/pq"
*/
package main

import (
	"fmt"
	"golang_db/models"

	"database/sql"

	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "*******"
	dbname   = "DATABASE"
)

var psqlinfo = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

func main() {
	// student := models.Student{Id: 9, Name: "Dian", Email: "dian@mail.com", Address: "Surabaya", Birth_date: time.Date(2002, 12, 2, 0, 0, 0, 0, time.Local), Gender: "F"}
	// addStudent(student)
	// updateStudent(student)
	// deleteStudent(8)
	// fmt.Println(getStudents())
	// fmt.Println(getStudentById(1))
	// fmt.Println(searchStudentBy("dam"))
	studentE := models.StudentEnrollment{Id: 1, Student_Id: 7, Credit: 5000, Subject: "Dark Of Tower Novel"}
	enrollSubject(studentE)
}

// DML
func addStudent(s models.Student) {
	db := connectToDb()
	defer db.Close()

	// EXECUTE QUERY TO DATABASE
	sqlStatement := "INSERT INTO mst_student (id, name, email, address, birth_date, gender) VALUES ($1, $2, $3, $4, $5, $6);"

	_, err := db.Exec(sqlStatement, s.Id, s.Name, s.Email, s.Address, s.Birth_date, s.Gender)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data Inserted!")
	}
	// EXECUTE QUERY TO DATABASE
}

func updateStudent(s models.Student) {
	db := connectToDb()
	defer db.Close()

	// EXECUTE QUERY TO DATABASE
	sqlStatement := "UPDATE mst_student SET name = $2, email = $3, address = $4, birth_date = $5, gender = $6 WHERE id = $1;"

	_, err := db.Exec(sqlStatement, s.Id, s.Name, s.Email, s.Address, s.Birth_date, s.Gender)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data Updated!")
	}
	// EXECUTE QUERY TO DATABASE
}

func deleteStudent(id int) {
	db := connectToDb()
	defer db.Close()

	// EXECUTE QUERY TO DATABASE
	sqlStatement := "DELETE FROM mst_student WHERE id = $1;"

	_, err := db.Exec(sqlStatement, id)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Data Deleted!")
	}
	// EXECUTE QUERY TO DATABASE
}

// DML

// DQL
func searchStudentBy(name string) []models.Student {
	db := connectToDb()
	defer db.Close()

	sqlStatement := "SELECT * FROM mst_student WHERE name ILIKE $1;"
	rows, err := db.Query(sqlStatement, "%"+name+"%")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	students := []models.Student{}
	scanStudent(rows, &students)
	return students
}

func getStudents() []models.Student {
	db := connectToDb()
	defer db.Close()

	sqlStatement := "SELECT * FROM mst_student;"
	rows, err := db.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	students := []models.Student{}
	scanStudent(rows, &students)
	return students
}

func getStudentById(id int) models.Student {
	db := connectToDb()
	defer db.Close()

	sqlStatement := "SELECT * FROM mst_student WHERE id = $1;"
	s := models.Student{}
	err := db.QueryRow(sqlStatement, id).Scan(&s.Id, &s.Name, &s.Email, &s.Address, &s.Birth_date, &s.Gender)
	if err != nil {
		panic(err)
	}

	return s
}

func scanStudent(rows *sql.Rows, students *[]models.Student) {
	for rows.Next() {
		s := models.Student{}
		err := rows.Scan(&s.Id, &s.Name, &s.Email, &s.Address, &s.Birth_date, &s.Gender)
		if err != nil {
			panic(err)
		}

		*students = append(*students, s)
	}

	err := rows.Err()
	if err != nil {
		panic(err)
	}
}

// DQL

// TRANSACTION / COMBINE MULTIPLE QUERY
func enrollSubject(se models.StudentEnrollment) {
	db := connectToDb()
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	insertStudentEnrollment(se, tx)
	taken_credit := getSumCreditOfStudentEnrollment(se.Student_Id, tx)
	updateStudentCredit(taken_credit, se.Student_Id, tx)

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func insertStudentEnrollment(se models.StudentEnrollment, tx *sql.Tx) {
	sqlStatement := "INSERT INTO tx_student_enrollment (id, student_id, subject, credit) VALUES ($1, $2, $3, $4)"

	_, err := tx.Exec(sqlStatement, se.Id, se.Student_Id, se.Subject, se.Credit)
	rollbackValidate(err, "Insert", tx)
}

func getSumCreditOfStudentEnrollment(id int, tx *sql.Tx) int {
	sqlStatement := "SELECT SUM(credit) FROM tx_student_enrollment WHERE student_id = $1;"

	takenCredit := 0
	err := tx.QueryRow(sqlStatement, id).Scan(&takenCredit)
	rollbackValidate(err, "Get Sum Credit of Student", tx)

	return takenCredit
}

func updateStudentCredit(takenCredit, id int, tx *sql.Tx) {
	sqlStatement := "UPDATE mst_student SET taken_credit = $1 WHERE ID = $2;"

	_, err := tx.Exec(sqlStatement, takenCredit, id)
	rollbackValidate(err, "Update Student Credit", tx)
}

func rollbackValidate(err error, msg string, tx *sql.Tx) {
	if err != nil {
		err = tx.Rollback()
		if err != nil {
			fmt.Println("Error when trying to rollback", err)
		} else {
			fmt.Println("Rollback: Transaction has been reverted!")
		}
	} else {
		fmt.Println("Successfully " + msg + "!")
	}
}

// TRANSACTION / COMBINE MULTIPLE QUERY

func connectToDb() *sql.DB {
	db, err := sql.Open("postgres", psqlinfo)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("connected with database")

	return db
}
