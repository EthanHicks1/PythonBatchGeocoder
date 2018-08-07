"""
Python script for batch geocoding large amounts of addresses. This script uses a goecoding package (geocoder)
to geocode address from ArcGIS and Komoot. The program needs all the variables in the configuration
section set before a user runs the script. This program does NOT require an API key. That said it is
still relatively fast without using an API key. It has been tested up to 50,000 in oue run. Without errors
it can geocode 100 addresses in 1-2 minutes.

Ethan Hicks
8-7-2018
"""

import geocoder
import requests
import time
import pandas as pd


#----------------------------- CONFIGURATION -----------------------------#

#Set the input and output files
input_file_path = "input.csv";
output_file_path = "output";

#Set the name of the column indexs here so that pandas can read the CSV file
address_column_name = "ADDRESS";
state_column_name = "STATE";
zip_column_name = "ZIP_CODE";   # Leave blank("") if you do not have zip codes

#Where the program starts processing the addresses in the input file
#This is useful in case the computer crashes so you can resume the program where it left off or so you can run multiple
#instances of the program starting at different spots in the file
start_index = 0;
#How often the program prints the status of the running program
status_rate = 100;
#How often the program saves a backup file
write_data_rate = 1000;
#How many times the program tries to geocode an address before it gives up
attempts_to_geocode = 3;
#Time it delays each time it does not find an address
#Note that this is added to itself each time it fails so it should not be set to a large number
wait_time = 3;

#----------------------------- Processing the input file -----------------------------#

df = pd.read_csv(input_file_path, low_memory=False);
#df = pd.read_excel(input_file_path)

#Raise errors if the provided column names could not be found in the input file
if address_column_name not in df.columns:
    raise ValueError("Can't find the address column in the input file.")
if state_column_name not in df.columns:
    raise ValueError("Can't find the state column in the input file.")

# Zip code is not needed but helps provide more accurate locations
if (zip_column_name):
    if zip_column_name not in df.columns:
        raise ValueError("Can't find the zip code column in the input file.")
    addresses = (df[address_column_name] + ', ' + df[zip_column_name].astype(str) + ', ' + df[state_column_name]).tolist();
else:
    addresses = (df[address_column_name] + ', ' + df[state_column_name]).tolist();


#----------------------------- Function Definitions -----------------------------#

# Creates request sessions for geocoding
class GeoSessions:
    def __init__(self):
        self.Arcgis = requests.Session();
        self.Komoot = requests.Session();

# Class that is used to return 3 new sessions for each geocoding source
def create_sessions():
    return GeoSessions();

# Main geocoding fucntion that uses the geocoding package to covert addresses into lat, longs
def geocode_address(address, s):
    g = geocoder.arcgis(address, session=s.Arcgis);
    if (g.ok == False):
        g = geocoder.komoot(address, session=s.Komoot);

    return g;


def try_address(address, s, attempts_remaining, wait_time):
    g = geocode_address(address, s);
    if (g.ok == False):
        time.sleep(wait_time)
        s = create_sessions();  # It is not very likely that we can't find an address so we create new sessions and wait
        if (attempts_remaining > 0):
            try_address(address, s, attempts_remaining-1, wait_time+wait_time);
    return g;


# Function used to write data to the output file
def write_data(data, index):
    file_name = (output_file_path + str(index) + ".csv");
    print("Created the file: " + file_name);
    done = pd.DataFrame(data);
    done.columns = ['Address', 'Lat', 'Long', 'Provider'];
    done.to_csv((file_name + ".csv"), sep=',', encoding='utf8');


# Variables used in the main for loop that do not need to be modified by the user
s = create_sessions();
results = [];
failed = 0;
total_failed = 0;
progress = len(addresses) - start_index;

#----------------------------- Main Loop -----------------------------#

for i, address in enumerate(addresses[start_index:]):
    # Print the status of how many addresses have be processed so far and how many of the failed.
    if ((start_index + i) % status_rate == 0):
        total_failed += failed;
        print(
            "Completed {} of {}. Failed {} for this section and {} in total.".format(i + start_index, progress, failed,
                                                                                     total_failed));
        failed = 0;

    # Try geocoding the addresses
    try:
        g = try_address(address, s, attempts_to_geocode, wait_time);
        if (g.ok == False):
            results.append([address, "was", "not", "geocoded"]);
            print("Gave up on address: " + address);
            failed += 1;
        else:
            results.append([address, g.latlng[0], g.latlng[1], g.provider]);

    #If we failed with an error like a timeout we will try the address again after we wait 5 secs
    except Exception as e:
        print("Failed with error {} on address {}. Will try again.".format(e, address));
        try:
            time.sleep(5);
            s = create_sessions();
            g = geocode_address(address, s);
            if (g.ok == False):
                print("Did not fine it.")
                results.append([address, "was", "not", "geocoded"]);
                failed += 1;
            else:
                print("Successfully found it.")
                results.append([address, g.latlng[0], g.latlng[1], g.provider]);
        except Exception as e:
            print("Failed with error {} on address {} again.".format(e, address));
            failed += 1;
            results.append([address, e, e, "ERROR"]);

    # Writing what has been processed so far to an output file
    if (i%write_data_rate == 0 and i != 0):
        write_data(results, i + start_index);

    #print(i, g.latlng, g.provider)


# Finished
write_data(results, i + start_index);
print("Finished! :)");
