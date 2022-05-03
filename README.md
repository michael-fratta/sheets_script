A Python script - running automatically, on a (hardcoded) scheduler; bundled as an app and hosted on the cloud platform Heroku - that, essentially, updates various Google Sheets with the contents of several CSV files fetched from an SFTP server. The steps it follows are explained - concisely - below (see code for full detail):

• defines relevant spreadsheet ranges to variables (for later use with the Google Sheets API) - to know where the data needs to be pasted into

• connects to an SFTP server, using the pysftp library, and attempts to get the latest files that match the provided search strings - reading them as a dataframe with pandas, if found

• shapes each of the dataframe versions of the CSV files in such a way for they to be used as data payloads within the Google Sheets API

• clears the existing contents of the provided spreadsheet ranges

• then, pastes the data in its place

I am the sole author of this script. Revealing keys/values/variables/file names have been replaced with arbitrary/generic ones - for demonstrative purposes only.
