import pandas as pd
import requests
import time
import os
import multiprocessing
import math
from bs4 import BeautifulSoup

pd.options.mode.chained_assignment = None  # default='warn'


def get_url_content(postcode, page):
    postcode = postcode.replace(' ', '-')
    url = 'http://www.zoopla.co.uk/house-prices/stalbans/cottonmill-lane/' + postcode + '/' + "?pn=" + str(page)
    data = requests.get(url)
    return data


def to_string(df):
    string = ""
    if not str(df["SAON"]).upper() == "NAN":
        if df["SAON"].isdigit():
            string = string + df["SAON"] + " "
        else:
            string = string + df["SAON"] + " "

    if not str(df["PAON"]).upper() == "NAN":
        if " - " in df["PAON"]:
            df["PAON"] = df["PAON"].replace(" - ", "-")
        if df["PAON"].isdigit():
            string = string + df["PAON"] + " "
        else:
            string = string + df["PAON"] + " "

    if not str(df["Street"]).upper() == "NAN":
        string = string + df["Street"]

    string = string.replace(",", " ")
    string = string.replace("  ", " ")
    return string


def gather(dataframe):
    print(multiprocessing.current_process().name)

    # Initialize page limit
    pages = 1

    # Loop until last page OR obtained the info for specific address
    for i in range(len(dataframe)):
        try:
            # Initialize the value per row
            bedroom = None
            bathroom = None
            reception = None
            wanted_address = to_string(dataframe.iloc[i])
            # print(wanted_address)
            found = False
            for j in range(1, pages + 1):
                # request data from website
                bs = BeautifulSoup(get_url_content(dataframe["Postcode"].iloc[i].replace(" ", "-"), j).content,
                                   "html.parser")

                # update page limit
                info = bs.find_all("div", {"class": "paginate bg-muted"})
                if len(info) == 1:
                    info = info[0].find_all("a")
                    if len(info) > 2:
                        pages = int(info[-2].text)

                room_info = bs.find_all("div", {"class": "hp-card-list"})

                if len(room_info) == 1:
                    room_info = room_info[0].find_all("section", {"class": "hp-card"})
                else:
                    break

                # Check if the address in this page
                for room in room_info:
                    searched_address = room.find("h3", {"class": "hp-card__title"}).string.strip().replace(",",
                                                                                                           "").upper()
                    # print(searched_address.replace(" " + dataframe["Postcode"].iloc[i], ""))
                    if wanted_address.strip() == searched_address.replace(" " + dataframe["Postcode"].iloc[i], ""):
                        print("Found")
                        # indicate room info found to exit traverse pages
                        found = True

                        # Bedroom
                        if room.find("li", {"class": "hp-card-room hp-card-room--bed"}) is not None:
                            bedroom = int(room.find("li", {"class": "hp-card-room hp-card-room--bed"})
                                          .text.replace("\n", "").replace("             ", "")
                                          .strip().replace("Bedrooms:", ""))

                        # Bathroom
                        if room.find("li", {"class": "hp-card-room hp-card-room--bath"}) is not None:
                            bathroom = int(room.find("li", {"class": "hp-card-room hp-card-room--bath"})
                                           .text.replace("\n", "").replace("             ", "")
                                           .strip().replace("Bathrooms:", ""))

                        # Reception
                        if room.find("li", {"class": "hp-card-room hp-card-room--recept"}) is not None:
                            reception = int(room.find("li", {"class": "hp-card-room hp-card-room--recept"})
                                            .text.replace("\n", "").replace("             ", "")
                                            .strip().replace("Reception rooms:", ""))
                        break

                # If room info found, find next room info.
                if found:
                    break

            dataframe.loc[dataframe.index[i], 'Bedroom'] = bedroom
            dataframe.loc[dataframe.index[i], 'Bathroom'] = bathroom
            dataframe.loc[dataframe.index[i], 'Reception'] = reception
            print(wanted_address)
            print(bedroom)
            print(bathroom)
            print(reception)
            print()
        except Exception:
            dataframe.loc[dataframe.index[i], 'Bedroom'] = "fail"
            dataframe.loc[dataframe.index[i], 'Bathroom'] = "fail"
            dataframe.loc[dataframe.index[i], 'Reception'] = "fail"
    dataframe.to_csv("./dataset/" + str(dataframe.index[0]) + ".csv")


def get_dfs():
    header_list = ["ID", "Price", "Date", "Postcode", "Property type", "New build",
                   "Estate type", "SAON", "PAON", "Street", "Locality",
                   "District", "Town", "County", "Transaction category", "Website"]
    files = os.listdir('./')
    dfs = []
    print("Loading csv...")
    for file in files:
        if ".csv" in file:
            df = pd.read_csv(os.path.abspath("./" + file), names=header_list)
            dfs.append(df)

    print("Merging csv...")
    df = pd.concat(dfs, axis=0, ignore_index=True)
    print("Selecting 24000 samples...")
    df = df.sample(24000, random_state=1)  # seed = 1

    print("Dropping columns and rows...")
    # drop rows that Postcode value is NaN
    df_price = df[df["Postcode"].isnull() == False]

    # Drop useless column for price dataframe
    df_price.drop(columns=["ID", "New build", "Postcode", "PAON", "SAON",
                           "Street", "Locality", "District", "Town",
                           "Transaction category", "Website"], inplace=True)

    # Drop rows that County value is NaN
    df_price.dropna(inplace=True)

    # Drop useless column for address dataframe
    df_address = df.drop(columns=["ID", "New build", "Property type", "Estate type", "Date",
                                  "Price", "Transaction category", "Website", "Town"])

    # Drop the same row in df_address
    df_address = df_address[df_address["County"].isnull() == False]
    df_address = df_address[df_address["Postcode"].isnull() == False]
    #
    print("Splitting dataset into 50 datasets...")
    df_address_list = []
    df_address.iloc[range(0, 40)]
    for i in range(1, 50):
        if i == 1:
            df_address_split = df_address.iloc[range(0, 480)]
            df_address_list.append(df_address_split)
        elif i == 49:
            df_address_split = df_address.iloc[range(480 * i, len(df_address))]
            df_address_list.append(df_address_split)
        else:
            df_address_split = df_address.iloc[range(480 * (i - 1), 480 * i)]
            df_address_list.append(df_address_split)
    return df_price, df_address


def split_df(df, num):
    df_list = []
    length = math.ceil(len(df_address) / num)
    for i in range(num):
        if i == 0:
            df_split = df.iloc[range(0, length)]
            df_list.append(df_split)
        elif i == num - 1:
            df_split = df.iloc[range(length * i, len(df))]
            df_list.append(df_split)
        else:
            df_split = df.iloc[range(length * i, length * (i + 1))]
            df_list.append(df_split)
    return df_list


if __name__ == '__main__':
    df_address = pd.read_csv("./address.csv", index_col=0)
    df_list = split_df(df_address, int(len(df_address)/10))
    print(int(len(df_list)))
    t1 = time.time()
    pool = multiprocessing.Pool(processes=5)
    pool.map(gather, df_list[2100:2130])
    # pool.map(gather, df_list[2228:len(df_list)])
    print(time.time() - t1)

    pool.close()
    pool.join()
