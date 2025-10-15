###########################################################
############# Setup before python coding ##################
###########################################################

#First we need to configure python environment and install brightway 2.5 packages
#We used anaconda navigator and PyCharm. So we need to download both software first.
#At the terminal window we need to enter the commands:
    #conda create -n environment name
    #conda activate environment_name
    #conda install brightway25 -c cmutel -c conda-forge
    #conda update -n base -c defaults conda

#In PyCharm, we create a project and we set up the interpreter according to your environment you just create.

pass

###########################################################
############# Setup brightway in python ###################
###########################################################

#This code requires BRIGHTWAY 2.5!!!
#Import:
import bw2data as bd
import bw2io as bi
import pandas as pd
from ecoinvent_interface import Settings, permanent_setting, EcoinventRelease, ReleaseType
import os
import re
import requests
import xlwings as xw


#__________________________________________________________________________
# Set up Brightway project
PROJECT_NAME = "bwei_311"
bd.projects.set_current(PROJECT_NAME)

# Define ecoinvent release version and system model + path
ei_release_version = "3.11"
system_model = "cutoff"
file_repository = '/Users/jmliesa/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/2 Ecoinvent Database/Ecoinvent repository/Ecoinvent Interface'

eidb_name = "ecoinvent-"+ ei_release_version+ "-"+ system_model
biodb_name= "ecoinvent-"+ ei_release_version+ "-"+ "biosphere"

#Login identification via the permanent_setting function:
#permanent_setting("username", "user")
#permanent_setting("password", "pass")
permanent_setting("output_path", file_repository)

#Configure ecoinvent-interface settings (secrets files read automatically)
my_settings = Settings()

# Use ecoinvent-interface to download or verify the release
release = EcoinventRelease(my_settings)
# Define the directory where specific ecoinvent releases are stored
release_dir = os.path.join(file_repository, "ecoinvent "+ ei_release_version+ "_"+ system_model+ "_ecoSpold02")

# Check if the release directory exists
if not os.path.exists(release_dir):
    release.get_release(ei_release_version, system_model, ReleaseType.ecospold)
    release.get_release(ei_release_version, system_model, ReleaseType.cumulative_lcia)
# Import ecoinvent release with multiprocessing disabled (crashes in an intel-based mac)
    bi.import_ecoinvent_release(ei_release_version, system_model, use_mp=False)

# Download and import the overview file:
def download_overview(ei_release_version, file_repository):
    urls = {
        "3.9.1": "https://19913970.fs1.hubspotusercontent-na1.net/hubfs/19913970/Database-Overview-for-ecoinvent-v3.9.1-9.xlsx",
        "3.10": "https://19913970.fs1.hubspotusercontent-na1.net/hubfs/19913970/Knowledge%20Base/Database/Releases/Database-Overview-for-ecoinvent-v3.10_29.04.24.xlsx",
        "3.10.1": "https://19913970.fs1.hubspotusercontent-na1.net/hubfs/19913970/Version%20Releases/3.10.1/Database-Overview-for-ecoinvent-v3.10.1.xlsx",
        "3.11": "https://19913970.fs1.hubspotusercontent-na1.net/hubfs/19913970/Knowledge%20Base/Database/Releases/3.11/Database-Overview-for-ecoinvent-v3.11%20(6).xlsx"
    }
    if ei_release_version not in urls:
        raise ValueError(f"Version {ei_release_version} not supported.")

    output_path = os.path.join(file_repository, f"Database-Overview-for-ecoinvent-v{ei_release_version}.xlsx")
    if not os.path.isfile(output_path):
        response = requests.get(urls[ei_release_version])
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
        else:
            raise Exception(f"Failed to download. Status code: {response.status_code}")

    return output_path

pass

#__________________________________________________________________________
#Directory paths:
#CHECK file_repository from bw setup!

implementation_file = os.path.join(file_repository, "ecoinvent "+ ei_release_version+ "_LCIA_implementation", "LCIA Implementation " + ei_release_version+ ".xlsx")
LCIAcutoff_file = os.path.join(file_repository, "ecoinvent "+ ei_release_version+ "_"+ system_model+ "_cumulative_lcia_xlsx", "Cut-off Cumulative LCIA v" + ei_release_version+ ".xlsx")
Ecoinventoverview_file = download_overview(ei_release_version, file_repository)

input_folder = "/Users/jmliesa/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/7 Scores/v3/Input"
Simapromapping_file = os.path.join(input_folder, "mapping_SimaPro_to_ecoinvent310.xlsx")
Scores_file = os.path.join(input_folder, "Scores_v3.xlsm")

output_folder = "/Users/jmliesa/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/7 Scores/v3/Output"


###########################################################
######### Exporting databases from brigthway ##############
###########################################################

#First way to do the databases:
ei = bd.Database(eidb_name).nodes_to_dataframe()
bio = bd.Database(biodb_name).nodes_to_dataframe()
#customdb = bd.Database("customdb_name").nodes_to_dataframe()


#Now we filter and replace unwanted characters from ei
#1/convert filename to uuid's
ei['filename'] = ei['filename'].replace(to_replace = '.spold', value='', regex=True)
ei.rename(columns={'filename':'Activity UUID_Product UUID'}, inplace=True)

#2/clean authors field
#ei['authors'] = ei['authors'].apply(lambda x: "|".join([f"{k}: {v}" for d in x for k,v in d.items()]))
ei['authors'] = ei['authors'].astype(str).str.replace(r"['{}()\[\]]", '', regex=True)

#3/clean other fields
ei['classifications'] = ei['classifications'].astype(str).str.replace(r"['()\[\]]", '', regex=True)
ei['synonyms'] = ei['synonyms'].astype(str).str.replace(r"['\[\]]", '', regex=True)
ei['parameters'] = ei['parameters'].astype(str).str.replace(r"['{}\[\]]", '', regex=True)


#Now we filter and replace unwanted characters from bio
bio[['compartment', 'subcompartment']] = pd.DataFrame(bio['categories'].tolist(), index=bio.index)
bio = bio.loc[:, ~bio.columns.isin(['categories'])].copy() # drop 'categories' column + add explicit copy to work separately of a bio dataframe

bio.loc[:, 'subcompartment'] = bio['subcompartment'].astype(str).str.replace('None', 'undefined', regex=True)

#Finally we can export databases:
ei.to_csv("ei.csv", index=True)
bio.to_csv("bio.csv", index=True)

#And we change fields so they can match afterwards
ei.rename(columns={'name':'Activity / Exchange name'}, inplace=True)
ei.rename(columns={'reference product':'Reference product / Compartment'}, inplace=True)
ei.rename(columns={'location':'Location / Subcompartment'}, inplace=True)

bio.rename(columns={'name':'Activity / Exchange name'}, inplace=True)
bio.rename(columns={'compartment':'Reference product / Compartment'}, inplace=True)
bio.rename(columns={'subcompartment':'Location / Subcompartment'}, inplace=True)

#The simapro name used for biosphere activities is indeed the same 'Activity / Exchange name' field, so we explicity
# create a field with this info, so we can afterwards merge it with ei metadata
bio['Simapro name'] = bio['Activity / Exchange name']

bio = bio[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment',
        'unit', 'type', 'CAS number', 'database', 'code', 'id', 'Simapro name']]

ei = ei[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment',
         'unit', 'type', 'classifications', 'comment', 'synonyms', 'production amount', 'parameters', 'authors',
        'Activity UUID_Product UUID', 'activity', 'activity type', 'flow', 'database', 'code',  'id']]


###########################################################
###### Creating brightway databases with IA scores ########
###########################################################

#__________________________________________________________________________
#biosphere CFs for all IA methods
cfs = {m: {bd.get_activity(key=tuple(key)).id: cf for key, cf in bd.Method(m).load()} for m in bd.methods}

CFSframe = pd.DataFrame(cfs)
CFSframe.index.name = "id"
CFSframe.columns = [' '.join(col) for col in CFSframe.columns]

bioframe = bio.merge(CFSframe, how="left", on=["id"])

#To correct the spelling issues of Ecoinvent methods we should  search and replace from column headers:
#This ONLY happens in ecoinvent versions <3.10, but just in case we keep the search / rename function here:
bioframe = bioframe.rename(columns={
    'ReCiPe 2016 v1.03, midpoint (H) no LT climate change no LT global warming potential (GWP1000) no LT': 'ReCiPe 2016 v1.03, midpoint (H) no LT climate change no LT global warming potential (GWP100) no LT',
    'ReCiPe 2016 v1.03, midpoint (H) climate change global warming potential (GWP1000)': 'ReCiPe 2016 v1.03, midpoint (H) climate change global warming potential (GWP100)',
    'ReCiPe 2016 v1.03, midpoint (I) no LT climate change no LT global warming potential (GWP1000) no LT': 'ReCiPe 2016 v1.03, midpoint (I) no LT climate change no LT global warming potential (GWP20) no LT',
    'ReCiPe 2016 v1.03, midpoint (I) climate change global warming potential (GWP1000)': 'ReCiPe 2016 v1.03, midpoint (I) climate change global warming potential (GWP20)'})


#__________________________________________________________________________
#calculate LCIA scores for all technosphere processes
#OPTION 1: From LCIA file

# Step 1: Read Excel with 4-level MultiIndex header
LCIAcutoff = pd.read_excel(LCIAcutoff_file, sheet_name="LCIA", header=[0,1,2,3])

# Step 2: Flatten headers using comma + parentheses
flattened_columns = [f"{col[0]}, {col[1]}, {col[2]} ({col[3]})" if pd.notna(col[3]) else f"{col[0]}, {col[1]}, {col[2]}"
    for col in LCIAcutoff.columns]
LCIAcutoff.columns = flattened_columns
flattened_columns[0] = 'Activity UUID_Product UUID'
LCIAcutoff.columns = flattened_columns

# Step 3: We delete columns we don't need a part from UUIDs
#We keep first colum (index 0) since it contains the Activity UUID_Product UUID information. We therefore skip activity name, ref product, location, etc.
#MAKE SURE the skipped cols names are coincident with the same header names in the ecoinvent LCIA file:
wildcards = ['Activity Name', 'Geography', 'Reference Product Name', 'Reference Product Unit', 'Reference Product Amount']
cols_to_drop = [col for col in LCIAcutoff.columns if any(w.lower() in col.lower() for w in wildcards)]

#Now we keep the LCIA scores in a clean dataframe with UUIDs ONLY!!!
#We keep it only with UUIDs to then do the merge with all ei fields.
technoframe_scores = LCIAcutoff.drop(columns=cols_to_drop)



#OPTIONS TO CREATE LCIA SCORES FROM BRIGHTWAY BELOW:
pass

# #OPTION 2: From spold files + brightway LCA score loop calculation
# #our database is:
# ecoinvent = bd.Database(eidb_name)
# ecoinvent = bd.Database(eidb_name).nodes_to_dataframe()
#
# results = {} #dictionary to store results
# for act in list(ecoinvent): #here we get the activity list from the ecoinvent database
#     results[act.id] = {}
#     lca_obj = act.lca(amount=1) #the inverted matrix is calculated here, for each activity at once
#     for m in bd.methods: #in the debugger, it will prompt m as the last method of the bd.methods database. Be careful this will calculate ALL methods in bd.methods
#         lca_obj.switch_method(m)
#         lca_obj.lcia()
#         results[act.id][m] = lca_obj.score
#
# technoframe = pd.DataFrame(results).transpose() #Here we can put results in a dataframe and then transpose it
# technoframe.index.name = "id"
#
# #OPTION 2 - EXAMPLE:
#
# ecoinvent = bd.Database(eidb_name)
# #As an example, here we use just 3:
# de = ecoinvent.get(name="market for electricity, medium voltage", location="DE")
# es = ecoinvent.get(name="market for electricity, medium voltage", location="ES")
# fr = ecoinvent.get(name="market for electricity, medium voltage", location="FR")
#
# results = {}
# for act in [de, es, fr]: #here we should put the [ei] database
#     results[act.id] = {}
#     lca_obj = act.lca(amount=1) #the inverted matrix is calculated here, for each activity at once
#     for m in bd.methods: #in the debugger, it will prompt m as the last method of the bd.methods database. Be careful this will calculate ALL methods in bd.methods
#         lca_obj.switch_method(m)
#         lca_obj.lcia()
#         results[act.id][m] = lca_obj.score
#
# technoframe = pd.DataFrame(results).transpose() #Here we can put results in a dataframe and then transpose it
# technoframe.index.name = "id"
#

pass


#__________________________________________________________________________
#ADDING OTHER EI METADATA AND SIMAPRO NAMES

#We also read Overview and Simapro names from overview & mapping files
overview_file = pd.read_excel(Ecoinventoverview_file, sheet_name='Cut-Off AO')
mapping_file = pd.read_excel(Simapromapping_file,sheet_name="mapping")

#Let's build a dictionary to map simapro names and then use them to include this info to the EI dataframe
mapping_dict = dict(zip(mapping_file['Activity UUID_Product UUID'], mapping_file['SimaPro_inventory_name']))
ei['Simapro name'] = ei['Activity UUID_Product UUID'].map(mapping_dict).fillna('')


#Let's build a dictionary to map overview file metadata with UUIDs
columns_to_map = ['ecoQuery URL', 'Time Period', 'Sector', 'ISIC Classification', 'CPC Classification', 'Product Information']
overview_dict = dict(zip(overview_file['Activity UUID & Product UUID'], overview_file[columns_to_map].apply(tuple, axis=1)))

mapped_cols_overviewfile = ei['Activity UUID_Product UUID'].map(overview_dict)
ei[columns_to_map] = mapped_cols_overviewfile.apply(lambda x: pd.Series(x) if isinstance(x, tuple) else pd.Series([''] * len(columns_to_map)))


#Now we have ALL metadata we need for ei, let's merge the metadata from bio too:
Scores_metadata = pd.concat([ei, bio])

#Re-arrange fields:
Scores_metadata = Scores_metadata[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment', 'unit',
                'synonyms', 'CAS number', 'Sector', 'classifications', 'ISIC Classification', 'CPC Classification',
               'production amount', 'Time Period', 'type', 'database', 'Activity UUID_Product UUID', 'activity',
               'activity type', 'flow', 'code', 'id', 'parameters', 'authors', 'Simapro name', 'ecoQuery URL',
               'Product Information', 'comment']]

#And select the fields we want for the Scores info df: (we add the .copy to ensure it's trated as a new df, not as a view of previous df)
Scores_info = Scores_metadata[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment', 'unit',
                'synonyms', 'CAS number', 'Sector', 'classifications', 'Time Period', 'database', 'Simapro name',
               'Activity UUID_Product UUID', 'ecoQuery URL', 'Product Information', 'comment']].copy()

#And to keep the names of processes only: (+.copy)
Scores_names = Scores_info[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment', 'unit', 'comment']].copy()


pass

#__________________________________________________________________________
#Now we join technosphere and bioshpere LCIA results + metadata

#This requires to first join ei results preserving left part of the ei dataframe
technoframe = pd.merge(ei, technoframe_scores, on="Activity UUID_Product UUID", how="left")

unmatched = technoframe[technoframe[technoframe.columns[99]].isna()]
print(f"Unmatched rows: {unmatched.shape[0]}")
print(unmatched['Activity UUID_Product UUID'].drop_duplicates().head())


#Re-arrange fields:
# Step 1: List the first fields you want to put at the front
first_fields = Scores_metadata.columns.tolist()
first_fields = [col for col in first_fields if col not in 'CAS number'] #we do not include bioshpere fields
# Step 2: Get all the other columns that are NOT in first_fields
remaining_fields = [col for col in technoframe.columns if col not in first_fields]
# Step 3: Reorder the DataFrame
technoframe = technoframe[first_fields + remaining_fields]


Scores_all = pd.concat([technoframe,bioframe])


#__________________________________________________________________________
#Now we clean all df's:

# Clean database quoting all text (+clean characters) to avoid the issue of \ characters that can skip columns in the
# ei df after the .nodes_to_dataframe.
# 1. Define a function to clean useless characters
def clean_text_keep_keywords(x):
    if isinstance(x, (dict, list, tuple)):
        x = str(x)  # Turn structured objects into string
    if isinstance(x, str):
        x = x.replace("\n", " ")  # Remove real newlines
        x = x.replace("\\", " ")  # Remove stray backslashes
        #x = x.replace("{", "")    # Remove curly braces
        #x = x.replace("}", "")
        #x = x.replace("[", "")    # Remove square brackets
        #x = x.replace("]", "")
        #x = x.replace("(", "")    # Remove parentheses
        #x = x.replace(")", "")
        x = x.replace("'", "")    # Remove single quotes
        #x = re.sub(' +', ' ', x)  # Replace multiple spaces with one space
        return x.strip()
    else:
        return x

# 2. Apply it to all object columns
for col in Scores_metadata.columns:
    if Scores_metadata[col].dtype == object:
        Scores_metadata[col] = Scores_metadata[col].apply(clean_text_keep_keywords)

for col in Scores_metadata.columns:
    if Scores_all[col].dtype == object:
        Scores_all[col] = Scores_all[col].apply(clean_text_keep_keywords)

for col in Scores_info.columns:
    if Scores_info[col].dtype == object:
        Scores_info[col] = Scores_info[col].apply(clean_text_keep_keywords)

for col in Scores_names.columns:
    if Scores_names[col].dtype == object:
        Scores_names[col] = Scores_names[col].apply(clean_text_keep_keywords)


#__________________________________________________________________________
#Now we export all df's to the Scores tool:


# Define the path to your Excel Macro-Enabled Workbook
Scores_file = os.path.join(input_folder, "Scores_v3.xlsm")

#Let's update the Scores_info so we already include a field to concatenate the first 4 fields (needed for the Scores tool).
Scores_info.insert(0, 'Concatenate', Scores_info.iloc[:, 0:4].apply(lambda x: ', '.join(x.astype(str)), axis=1))


# Write to the file without overwriting macros
sheet_name = 'Scores_matrix_info'
startrow = 0    # Starts from the very first row (Excel is 1-based, Python is 0-based)
startcol = 0    # Starts from the second column (Excel "B")
chunk_size = 3000  # Number of rows to write per chunk
# === Open Workbook with xlwings ===
wb = xw.Book(Scores_file)
sheet = wb.sheets[sheet_name]

# === Clean the cells from the 2nd row onwards ===
last_row = sheet.cells.last_cell.row
last_col = sheet.cells.last_cell.column
print(f"Cleaning range A2 to {last_row}, {last_col}")
sheet.range((2, 1), (last_row, last_col)).clear_contents()

# === Write headers ===
sheet.range((startrow + 1, startcol + 1)).value = Scores_info.columns.tolist()
# === Chunked writing of the DataFrame ===
for i in range(0, len(Scores_info), chunk_size):
    chunk = Scores_info.iloc[i:i + chunk_size]
    print(f"Writing rows {i} to {i + len(chunk)}...")  # Log progress
    sheet.range((startrow + 2 + i, startcol + 1)).value = chunk.values.tolist()

#Let's delete the cocatenate field we needed:
Scores_info.drop(columns=['Concatenate'], inplace=True)


#!!!!!!!
#Manually SAVE the scores tool xlsm file as desired.
#Remember to place the Scores matrix at the same folder.


#__________________________________________________________________________
#Now we export all df's:

#Export the whole database (metadata + all LCIA Scores) -> around 150MB for Scores_all in xlsx, better filter it!
#Scores_metadata includes ALL LCI's metadata, while info only the selected ones.
Scores_all.to_excel(f"{output_folder}/Scores_all_{ei_release_version}.xlsx", index=False)
Scores_metadata.to_excel(f"{output_folder}/Scores_metadata_{ei_release_version}.xlsx", index=False)

#And summarized info / names from BioTechno matrix only:
Scores_info.to_excel(f"{output_folder}/Scores_info_{ei_release_version}.xlsx", index=False)
Scores_names.to_excel(f"{output_folder}/Scores_names_{ei_release_version}.xlsx", index=False)



#If we want to export just certain LCIA scores:
Scores_info_cols = len(Scores_info.columns)
metadata_cols = list(Scores_all.iloc[:, :Scores_info_cols].columns)

#From ReCiPe method, (H) perspective, including Long Term (LT) emissions, we have:
recipe_methods = [i for i in Scores_all.columns if 'ReCiPe' in i and '(H)' in i and 'no LT' not in i]

cols_to_drop = [col for col in Scores_all.columns if col not in metadata_cols and col not in recipe_methods]
Scores_recipe = Scores_all.drop(columns=cols_to_drop)

#if we want to export CED methods:
CED_methods = [i for i in Scores_all.columns if 'CED' in i]

#together with other selected methods:
EF_methods = [i for i in Scores_all.columns if 'EF v3.1' in i and 'no LT' not in i]
cols_to_drop = [col for col in Scores_all.columns if col not in metadata_cols and col not in EF_methods and col not in CED_methods and col not in recipe_methods]
Scores_recipe_ced_ef = Scores_all.drop(columns=cols_to_drop)


#If we want to use this as the Scores_matrix for the Scores tool, we need to add a concatenate field:
Scores_recipe_ced_ef.insert(0, 'Concatenate', Scores_recipe_ced_ef.iloc[:, 0:4].apply(lambda x: ', '.join(x.astype(str)), axis=1))

#And then export again:
Scores_recipe_ced_ef.to_excel(f"{output_folder}/Scores_matrix_{ei_release_version}.xlsx", sheet_name='Scores_matrix', index=False)


#__________________________________________________________________________
#End of the code :)

pass
