# DNA - Database Neutral Access

Simple Database wrapper for Go. It works somewhat like an ORM, but I think it's too much simple to call it an ORM.

This package was originally called HashSqlite, but this name caused some confusion driving people to think that there was some kind of hash function involved.
Also it has attached the project to Sqlite. It was a fact that it only wrapped Sqlite at start, but now it also wraps Oracle. Also, other databases may be wrapped by DNA in the future.


```Go

   .
   .
   .

import(
	"github.com/luisfurquim/dna"
)


   .
   .
   .


// Define your schema along with your data types

   .
   .
   .

type AddressBook struct {
	// Define a table name using a 'table' tag on a dna.TableName type
	TName               dna.TableName `table:"address_book" json:"-"`

	// Define a primary key with a dna.PK type
	// Optionally use a 'field' tag to define the column name, if not provided, the column will be named after the field name
	// Optionally use a 'prec' tag to define a precision (currently it is used only by Oracle databases, SQLite ignores it)
	Id 	              dna.PK	 	    `field:"id" prec:"6" json:"id,omitempty"`

	// Define other fields using Go native types and, optionally, field and/or prec tags
	Name                string		    `field:"name" prec:"80" json:"name,omitempty"`
	Nick					  string		    `field:"nick" prec:"25" json:"name,omitempty"`

	// Use array of pointers to other table types to define 1xM relations
	Addresses        []*Address	    `json:",omitempty"`

	// Define 'select' prepared statements using dna.Find types
	// Use the 'cols' tag to list column names to fetch
	// joins are defined by using the relation fields followed by a colon and the name of a 'select' prepared statement defined in the related type
	AllById            dna.Find      `cols:"id,name,nick,Addresses:ById" by:"id==<-id" limit:"1" json:"-"`
	AllByStatus        dna.Find      `cols:"id,name,nick,Addresses:ByStatus" sort:"name" json:"-"`
}

type AddressList struct {
	TName              dna.TableName `table:"address_list" json:"-"`
	Id 	             dna.PK		   `field:"id" prec:"10" json:"id,omitempty"`
	Address            string		   `field:"address" prec:"1900" json:"address,omitempty"`
	AddressType			 string		   `field:"address_type" prec:"20" json:"address,omitempty"`
	Status             string        `field:"status" prec:"6" json:"status,omitempty"`
	AddressBook       *AddressBook   `json:",omitempty" swagger:"-"`
	ById			       dna.Find      `cols:"id,address,status" by:"id==<-id_Addresses" json:"-"`
	ByStatus		       dna.Find      `cols:"id,address" by:"id==<-id_Addresses && status==<-stat" json:"-"`
}

```


```Go

   .
   .
   .

// Connect with your oracle database
   .
   .
   .

import(
	"time"
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
	"github.com/luisfurquim/dna/dnaoracle"
)

   .
   .
   .

	var db *go_ora.Connection
	var dnadb *dna.Dna
	var host, service, user, pw string
	var port int
	var addressBook AddressBook
	var address Address

	host = "hostname or ip of Oracle server here"
	port = 1521 // for majority of cases ...
	service = "service name used on your database"
	user = "login name"
	pw = "secret"

	dbName = go_ora.BuildUrl(host, port, service, user, pw, nil)

	db, err = go_ora.NewConnection(dbName, nil)
   if err != nil {
      dna.Goose.Init.Fatalf(1,"Error connecting with oracle via %s@%s: %s", user, host, err)
   }

   err = db.Open()
   if err != nil {
      dna.Goose.Init.Fatalf(1,"Erro abrindo oracle: %s", err)
   }

	dnadb, err = dna.New(dnaoracle.New(db), dna.Schema{
		Tables: []any{
			AddressBook{},
			AddressList{},
		},
	})
	if err != nil {
		dna.Goose.Init.Fatalf(1,"Error initializing DNA: %s", err)
	}

	defer dnadb.Close()

   .
   .
   .

```

```Go

   .
   .
   .

// Or open your SQLite database
   .
   .
   .

import(
	"time"
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
	"github.com/luisfurquim/dna/dnasqlite"
)

   .
   .
   .

	var db *sqlite.Conn
	var dnadb *dna.Dna

	host = "hostname or ip of Oracle server here"
	port = 1521 // for majority of cases ...
	service = "service name used on your database"
	user = "login name"
	pw = "secret"

	dbName = "file:/go/testdrive.sqlite"

   _, err = os.Stat(dbName)
   if err!=nil  && !os.IsNotExist(err) {
      dna.Goose.Init.Fatalf(1,"Erro checando anakin.sqlite: %s", err)
   }

   db, err = sqlite.Open(dbName, sqlite.OpenReadWrite | sqlite.OpenCreate)
   if err != nil {
      dna.Goose.Init.Fatalf(0,"Erro abrindo anakin.sqlite: %s", err)
   }

	dnadb, err = dna.New(dnasqlite.New(db), dna.Schema{
		Tables: []any{
			AddressBook{},
			AddressList{},
		},
	})
	if err != nil {
		dna.Goose.Init.Fatalf(1,"Error initializing DNA: %s", err)
	}

	defer dnadb.Close()

   .
   .
   .


```


```Go

	var addressBook AddressBook
	var addressList []*AddressBook
	var address Address

	// Initialize your data

	addressBook = &AddressBook{
		Name: "John Doe",
		Nick: "JD",
		Addresses: []*AddressList{
			&AddressList{
				Address: "https://example.com/johndoe",
				AddressType: "SOCIAL",
				Status: "",
			},
			&AddressList{
				Address: "johndoe@example.com",
				AddressType: "EMAIL",
			},
			&AddressList{
				Address: "42, Doe Street - Johnny Town",
				AddressType: "Home",
			},
		},
	}

	// Save it to the database. As Id is zero, DNA uses 'INSERT' clause, creating new rows. If Id was set, DNA would use 'UPDATE' clause.
	// All the rows listed in 'Addresses' will be saved too (and because their Ids are zero, they also will be inserted in the database
	addressBook.Id, err = dnadb.Save(addressBook)
	if err != nil {
		dna.Goose.Init.Fatalf(1,"Error saving address book entry: %s", err)
	}

   .
   .
   .

	// Fetch it from database providing:
	// 1) an array to hold the columns retrieved
	// 2) the rule name (defined when u declared your type with a 'dna.Find' struct field)
	// 3) optionally, the bind values if you defined them when declared the rule with a 'dna.Find' struct field
	err = dnadb.Find(dna.At{
		Table: &addressList,
		With: "AllById",
		By: map[string]any{
			"id": 1,
		},
	})


```

