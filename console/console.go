package console

import "fmt"

func Start() {
	for {
		fmt.Print("--Main menu--\n 1. PUT\n 2. GET\n 3. DELETE\n 4. INFO\n 0. EXIT\n Choose one of the options above: ")
		var input int
		_, error := fmt.Scan(&input)
		if error != nil {
			fmt.Println("The input is not integer! ERROR -> ", error)
			continue
		}
		switch input {
		case 1:
			Put()
		case 2:
			Get()
		case 3:
			Delete()
		case 4:
			fmt.Println("--ABLE FUNCTIONS--\nPUT - putting key:value pair into the program\nGET - geting the value based on the given key\nDELETE - deleting the key along side it's value")
			fmt.Println("AGREEMENT: Pair key:value from the perspective of the user are both in type string, but after the input, program restore the value into binary form.\n ...")
		case 0:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Your input is invalid!")
		}
	}
}

func Put() (string, string) {
	fmt.Println("Enter the key: ")
	var inputKey string
	fmt.Scan(&inputKey)
	fmt.Println("Enter the value: ")
	var inputValue string
	fmt.Scan(&inputValue)
	binInputValue := stringToBin(inputValue)
	fmt.Println(binInputValue) // Writing the binary form, just for the sakes of not giving error
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	return inputKey, inputValue
}

func Get() string {
	fmt.Println("Enter the key:")
	var inputKey string
	fmt.Scan(&inputKey)
	// HERE WE NEED TO IMPLEMENT GETTING THE VALUE (for now only to write it on wal)
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	var value string
	return value
}

func Delete() {
	fmt.Println("Enter the key:")
	var inputKey string
	fmt.Scan(&inputKey)
	// HERE WE NEED TO GET THE VALUE BASED ON THE KEY ALONGSIDE DELETING BOTH FROM MEMORY AND DISK IF IT'S PERMANENT (?)
	// MISSING THE APPROVE FROM WAL, DATA NEED TO BE SEND TO THE WAL WERE IT WILL BE STORED TILL DISMISED TO THE DISK
	var value string
	fmt.Println("Value " + value + " with the key " + inputKey + " was deleted.")
}

func stringToBin(s string) (binString string) {
	for _, c := range s {
		binString = fmt.Sprintf("%s%b", binString, c)
	}
	return
}
