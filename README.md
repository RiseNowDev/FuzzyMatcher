# Fuzzy Supplier Matcher

## Overview

This Python program is designed to help businesses find potential matches among their supplier names. It's particularly useful when dealing with large lists of suppliers where names might be slightly different but actually refer to the same company. The program uses advanced text comparison techniques to identify these potential matches.

## What Does This Program Do?

1. **Reads Supplier Data**: The program connects to a database that contains a list of supplier names.

2. **Cleans Up Names**: It standardizes all supplier names by removing numbers, special characters, and extra spaces. This helps in comparing names more accurately.

3. **Compares Names**: Using a technique called "fuzzy matching," the program compares each supplier name with all other names in the database. This method can identify similarities even when names aren't exactly the same.

4. **Identifies Potential Matches**: When two supplier names are found to be very similar (based on a score you can set), the program marks them as potential matches.

5. **Saves Results**: All potential matches are saved back to the database for later review.

6. **Provides Progress Updates**: As the program runs, it gives regular updates on how many suppliers it has processed and how long it's taking.

## Why Is This Useful?

- **Reduce Duplicates**: It helps identify potential duplicate suppliers in your database.
- **Improve Data Quality**: By finding similar names, you can standardize your supplier list.
- **Save Time**: Instead of manually comparing thousands of names, this program does it quickly and efficiently.

## How to Use the Program

1. **Set Up**: Make sure you have the program installed on your computer and connected to your supplier database.

2. **Run the Program**: Open a command prompt or terminal and navigate to the folder containing the program. Then type `python main.py` and press Enter.

3. **Enter Settings**: The program will ask you for three pieces of information:
   - Matching Score: How similar names need to be to be considered a match (default is 85%).
   - Number of Cores: How much of your computer's processing power to use (leave blank for maximum).
   - Batch Size: How many suppliers to process at once (default is 200,000).

4. **Wait for Processing**: The program will start running. You'll see updates on its progress.

5. **Review Results**: Once finished, the program will show you statistics about how many suppliers were processed and how many potential matches were found.

## Technical Details (for IT Support)

- This program is written in Python and uses PostgreSQL as its database.
- It requires several Python libraries including pandas, rapidfuzz, and SQLAlchemy.
- The program uses multiprocessing to speed up the matching process on computers with multiple cores.
- Results are stored in a new table in the database, with a timestamp in the table name.

## Important Notes

- Processing large numbers of suppliers can take a long time, depending on your computer's speed and the size of your supplier list.
- The program requires a stable connection to your database throughout the process.
- It's recommended to run this program during off-hours if you're dealing with a very large supplier list.

## Support

If you encounter any issues or have questions about using this program, please contact your IT support team or the software developer.