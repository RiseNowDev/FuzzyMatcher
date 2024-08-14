# Fuzzy Supplier Matcher

## Overview

This Python program helps businesses find potential matches among their supplier names. It's especially useful for large supplier lists where company names might be slightly different but actually refer to the same supplier. The program uses smart text comparison techniques to spot these potential matches.

## What Does This Program Do?

1. **Reads Supplier Data**: Connects to your database containing supplier names.

2. **Cleans Up Names**: Makes all supplier names consistent by removing numbers, special characters, and extra spaces.

3. **Compares Names**: Uses "fuzzy matching" to compare each supplier name with all others, finding similarities even when names aren't exactly the same.

4. **Finds Potential Matches**: Marks supplier names as potential matches when they're very similar (you can set how similar they need to be).

5. **Saves Results**: Stores all potential matches in the database for you to review later.

6. **Shows Progress**: Gives you updates on how many suppliers it has processed and how long it's taking.

## Why Is This Useful?

- **Find Duplicates**: Helps you spot potential duplicate suppliers in your list.
- **Clean Up Data**: Helps you standardize your supplier names.
- **Save Time**: Compares thousands of names quickly, saving you from doing it manually.

## How to Use the Program

1. **Set Up**: Make sure the program is installed and connected to your supplier database.

2. **Run the Program**: Open a command prompt or terminal, go to the program's folder, and type `python main.py`.

3. **Choose Settings**:
   - Matching Score: How similar names should be to count as a match (85% is the default).
   - Number of Cores: How much of your computer's power to use (leave blank to use maximum).
   - Batch Size: How many suppliers to process at once (200,000 is the default).

4. **Wait**: The program will run and show you updates on its progress.

5. **Check Results**: When it's done, you'll see how many suppliers were processed and how many potential matches were found.

## Technical Details (for IT Support)

- Uses Python and PostgreSQL database.
- Needs Python libraries: pandas, rapidfuzz, SQLAlchemy, psycopg2, and others.
- Uses multiprocessing to work faster on computers with multiple cores.
- Creates a new table in the database to store results, with a timestamp in the table name.
- Includes test files (test_aid.py, test_file_staging.py, test_main.py) for verifying the program's functions.

## Important Notes

- It might take a long time if you have a lot of suppliers.
- Needs a stable connection to your database the whole time it's running.
- Best to run it during off-hours if you have a very large supplier list.
- The program now includes more detailed error logging and progress updates.

## New Features

- Improved memory usage tracking.
- Better handling of database connections and transactions.
- More detailed logging of the matching process.
- Creation of a joined view in the database for easier result analysis.

## Support

If you have any problems or questions about using this program, please contact your IT support team or the software developer. They can help you with running the program, interpreting results, or troubleshooting any issues.