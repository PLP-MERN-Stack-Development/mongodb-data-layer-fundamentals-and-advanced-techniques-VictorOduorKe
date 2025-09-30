const { MongoClient } = require("mongodb");

const uri = 'mongodb://localhost:27017';

const dbName = 'plp_bookstore';
const collectionName = 'books';

///---reusable database connection function
async function dbConnection() {
    const client = new MongoClient(uri);
    await client.connect();
    console.log("conneted to server");


    // Get database and collection
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    return { collection, client };

};

//-----function to find all fiction books
async function findSpecificGenre() {
    const { collection, client } = await dbConnection();
    try {
        //get all documents
        const specificGenre = await collection.aggregate([{ $match: { genre: "Fiction" } }]).toArray();
        console.log("All fiction books: ", specificGenre)

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}

//findSpecificGenre()


//----- function to querry all books published after 1950
async function publishedAfter1995() {
    const { collection, client } = await dbConnection();

    try {
        const booksPublishedAfter1995 = await collection.aggregate([{ $match: { published_year: { $gte: 1950 } } }]).toArray();
        console.log("ALl books published after 1995: ", booksPublishedAfter1995)
    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}

//publishedAfter1995()

///----- function to find books by specific author
async function booksBYSpecisficAuthor(name) {
    const { collection, client } = await dbConnection();

    try {
        const specificAuthor = await collection.aggregate([{ $match: { author: name } }]).toArray();
        console.log("Books published by Paulo Coelho: ", specificAuthor);

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}
//booksBYSpecisficAuthor("Paulo Coelho");


///=----------function to update a specific book title
async function updateBookPrice() {
    const { collection, client } = await dbConnection();

    try {
        const updateBook = await collection.updateOne(
            { title: "The Alchemist" },
            { $set: { price: 11.99 } }
        );
        console.log("Book updated: ", updateBook);

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}
//updateBookPrice()

// functioon to delete book by title

async function deleteBook() {
    const { collection, client } = await dbConnection();

    try {
        const delete_book = await collection.deleteOne(
            { title: "Animal Farm" }
        );
        console.log("Book deleted: ", delete_book);

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}
//deleteBook();

//------### Task 3: Advanced Queries
////-----query to find books that are both in stock and published after 2010

async function bothInStockPublishedAfter2010() {
    const { collection, client } = await dbConnection();

    try {
        const query = await collection.find({
            in_stock: true,
            published_year: { $gte: 2010 }
        }
        ).toArray();
        console.log("Both in stock and published after 2010: ", query);

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}
//bothInStockPublishedAfter2010()

///------projection to return only the title, author, and price fields in your queries
async function projectionFunc() {
    const { collection, client } = await dbConnection();

    try {
        const query = await collection.aggregate(
            [{ $project: { title: "$title", author: "$author", price: "$price" } }]
        ).toArray();
        console.log("Books: ", query);

    } catch (error) {
        console.error("ERror has occured: ", error)
    } finally {
        await client.close();
    }
}
//projectionFunc()


//-------sorting to display books by price (both ascending and descending)
//-----AScending order
async function sortBooksAscending() {
    const { collection, client } = await dbConnection();

    try {
        let query = await collection
            .find({})
            .sort({ price: 1 })
            .toArray();
        console.log("Assecnding Order ", query);

    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}

//sortBooksAscending();

//----descending order
async function sortBooksDescending() {
    const { collection, client } = await dbConnection();

    try {
        let query = await collection
            .find({})
            .sort({ price: -1 })
            .toArray();
        console.log("Descecnding Order ", query);

    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}

//sortBooksDescending();


//------`limit` and `skip` methods to implement pagination (5 books per page)


async function getBooksByPage(pageNUmber) {
    const { collection, client } = await dbConnection();
    const pageSize = 3;
    const skipCount = (pageNUmber - 1) * pageSize;
    try {
        let query = await collection
            .find({})
            .skip(skipCount)
            .limit(pageSize)
            .toArray();
        console.log(`Page ${pageNUmber} book`, query);

    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}

//getBooksByPage(4);

////------------### Task 4: Aggregation Pipeline

//- Create an aggregation pipeline to calculate the average price of books by genre

async function calculateAvgPriceByGenre() {
    const { collection, client } = await dbConnection();
    try {
        const query = await collection
            .aggregate([
                {
                    $group: {
                        _id: "$genre",
                        averagePrice: { $avg: "$price" }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        genre: "$_id",
                        averagePrice: { $round: ["$averagePrice", 2] }
                    }
                }
            ])
            .toArray();
        console.log(`Average price of books by genre`, query);

    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}
//calculateAvgPriceByGenre();

//--------aggregation pipeline to find the author with the most books in the collection

async function authorWithMostBooks() {
    const { collection, client } = await dbConnection();
    try {
        const query = await collection.aggregate([
            {
                $group: {
                    _id: "$author",
                    totalBooks: { $sum: 1 }
                }
            },
            {
                $sort: { totalBooks: -1 }
            },
            { $limit: 1 },
            {
                $project: {
                    _id: 0,
                    author: "$_id",
                    totalBooks: 1
                }
            }
        ]).toArray();

        console.log("Author with most books: ", query)
    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}
//authorWithMostBooks();

///-------Implement a pipeline that groups books by publication decade and counts them

async function publicationDecade() {
    const { collection, client } = await dbConnection();

    try {

        const query = await collection.aggregate([
            {
                $project: {
                    title: 1,
                    published_year: 1,
                    decade: [
                        { $toString: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] } },
                        "s"
                    ]
                }
            }, {
            $group: {
                _id: "$decade",
                totalBooks: { $sum: 1 }
            }
        },
            { $sort: { _id: 1 } }
        ]).toArray()
        console.log("Books that have taken a decade since published: ", query)
    } catch (error) {
        console.error("Error has occured: ", error)

    } finally {
        await client.close();
    }
}
//publicationDecade();

//-----### Task 5: Indexing

//---- Create an index on the `title` field for faster searches
async function createIndexTitle(){
    const { collection, client } = await dbConnection();
    try {
        const query=await collection.createIndex("title")

        console.log("index created for the title: ", query)
    } catch (error) {
        console.error("Error has occured: ", error)
    } finally {
        await client.close();
    }
}
//createIndexTitle()

//-----Create a compound index on `author` and `published_year`

async function coumpoundIndex(){
    const {collection,client}=await dbConnection();

    try {
        const query=await collection.createIndex({author:1,published_year:-1});
        if(!query){
            console.log("Counpound index not created: ",query);
        return
    }
    console.log("Counpound index created: ",query);
        
    } catch (error) {
        console.log("An error has occured: ",error)
    }finally{
        await client.close();
    }
}
//coumpoundIndex()