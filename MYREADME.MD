# Running `queries.js`

This project contains a `queries.js` file with multiple MongoDB query functions. To help you run and observe each query properly, please follow the instructions below.

## Prerequisites
- Node.js installed on your system
- MongoDB server running and accessible
- All required npm packages installed (run `npm install` if needed)

## How to Run Queries
1. **Open `queries.js` in your code editor.**
2. **Each query is implemented as a separate function.**
   - At the bottom of the file, you will find a list of function calls (one for each query).
   - By default, these function calls may be commented out.
3. **To execute a specific query:**
   - Uncomment the function call for the query you want to run.
   - Make sure only one function call is uncommented at a time. This ensures you can observe the output for each query separately.
4. **Save the file after uncommenting the desired function call.**
5. **Run the file using Node.js:**
   ```bash
   node queries.js
   ```
6. **Observe the output in your terminal.**
7. **Repeat steps 3-6 for each query you want to test.**

## Example
Suppose you have the following at the bottom of `queries.js`:
```js
// findAllBooks();
// findBooksByAuthor('John Doe');
```
To run the `findAllBooks` query, change it to:
```js
findAllBooks();
// findBooksByAuthor('John Doe');
```
Then run:
```bash
node queries.js
```

## Notes
- Only run one query function at a time for best results.
- Make sure your MongoDB server is running and accessible.
- If you encounter errors, check your database connection settings in the file.

---
**Happy querying!**