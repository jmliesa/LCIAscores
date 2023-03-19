###########################################################
############# Setup before python coding ##################
###########################################################

#First we need to configure python environment and install brightway 2.5 packages
#We used anaconda navigator and PyCharm. So we need to download both softwares first.
#At the terminal window we need to enter the commands:
    #conda create -n environment name
    #conda activate environment_name
    #conda install brightway25 -c cmutel -c conda-forge
    #conda update -n base -c defaults conda

#In PyCharm, we create a project and we set up the interpreter according to your environment you just create.


###########################################################
############# Setup brightway in python ###################
###########################################################

#Directory paths:
spold_files = "/Users/joan/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/2 Ecoinvent Database/Ecoinvent repository/Version 3.9.1/Ecospold/datasets391"
implementation_file = "/Users/joan/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/2 Ecoinvent Database/Ecoinvent repository/Version 3.9.1/LCIA Implementation 3.9.1.xlsx"
LCIAcutoff_file = "/Users/joan/University Dropbox/Joan Muñoz/Feina Camins/Biblioteca/LCA/2 Ecoinvent Database/Ecoinvent repository/Version 3.9.1/Cut-off Cumulative LCIA v3.9.1.xlsx"

#Import:
import bw2data as bd
import bw2io as bi
import pandas as pd

#If we do not remember the name of our project we can always list it:
#print(bd.projects)

#Here we setup our project as current
bd.projects.set_current("bwei_391")

# #And we setup brightway, which will create the biosphere and deafult methods (from ecoinvent)
# bi.bw2setup()
#
# #And then we create the technosphere from spold files:
# ei = bi.SingleOutputEcospold2Importer(spold_files,"cutoff391", use_mp=False)
# ei.apply_strategies()
# ei.write_database()
#
# #To list all databases and methods from the project:
# print(bd.databases)
# print(bd.methods)
#
# #rember LCIA methods are already imported since they are build-in brightway. However, they are imported from Ecoinvent already.
# #however, if there is some problem with it, we can:
# bi.create_default_lcia_methods(overwrite=True)


###########################################################
######### Exporting databases from brigthway ##############
###########################################################

#First way to do the databases:
ei = bd.Database("cutoff391").nodes_to_dataframe()
bio = bd.Database("biosphere3").nodes_to_dataframe()
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
bio = bio.loc[:, ~bio.columns.isin(['categories'])] # drop 'categories' column

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


bio = bio[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment',
        'unit', 'type', 'CAS number', 'database', 'code', 'id']]

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
bioframe = bioframe.rename(columns={
    'ReCiPe 2016 v1.03, midpoint (H) no LT climate change no LT global warming potential (GWP1000) no LT': 'ReCiPe 2016 v1.03, midpoint (H) no LT climate change no LT global warming potential (GWP100) no LT',
    'ReCiPe 2016 v1.03, midpoint (H) climate change global warming potential (GWP1000)': 'ReCiPe 2016 v1.03, midpoint (H) climate change global warming potential (GWP100)',
    'ReCiPe 2016 v1.03, midpoint (I) no LT climate change no LT global warming potential (GWP1000) no LT': 'ReCiPe 2016 v1.03, midpoint (I) no LT climate change no LT global warming potential (GWP20) no LT',
    'ReCiPe 2016 v1.03, midpoint (I) climate change global warming potential (GWP1000)': 'ReCiPe 2016 v1.03, midpoint (I) climate change global warming potential (GWP20)'})


#__________________________________________________________________________
#calculate LCIA scores for all technosphere processes
#OPTION 1: From LCIA file

#MAKE SURE the skipped cols are the same in the ecoinvent LCIA file: check if methods start at column index 6! (cell G or number 7 in excel file)
skipped_cols = [1,2,3,4,5]

LCIAcutoff = pd.read_excel(LCIAcutoff_file, sheet_name="LCIA", header=[0,1,2], skiprows=[3])
LCIAcutoff.columns = [' '.join(col).strip() for col in LCIAcutoff.columns.values]
LCIAcutoff.columns.values[0] = 'Activity UUID_Product UUID'
LCIAcutoff = LCIAcutoff.drop(LCIAcutoff.columns[skipped_cols], axis=1)

#And we join results preserving left part of the ei dataframe
technoframe = pd.merge(ei, LCIAcutoff[['Activity UUID_Product UUID'] + list(LCIAcutoff.columns[6:])],
                       on="Activity UUID_Product UUID", how="left")


#OPTIONS TO CREATE LCIA SCORES FROM BRIGHTWAY BELOW:
pass

# #OPTION 2: From spold files + brightway LCA score loop calculation
# #our database is:
# ecoinvent = bd.Database("cutoff391")
#
# results = {} #dictionary to store results
# for act in list(ecoinvent): #here we get the qctivity list from the ecoinvent database
#     results[act.id] = {}
#     lca_obj = act.lca(amount=1) #the inverted matrix is calculated here, for each activity at once
#     for m in bd.methods: #in the debugger, it will prompt m as the last method of the bd.methods database. Be careful this will calculate ALL methods in bd.methods
#         lca_obj.switch_method(m)
#         lca_obj.lcia()
#         results[act.id][m] = lca_obj.score
#
# technoframe = pd.DataFrame(results).transpose() #Here we can put results in a dataframe and then transpose it
# technoframe.index.name = "id"

# #OPTION 2 - EXAMPLE:
# #As an example, here we use just 3:
# de = bd.Database("cutoff39").get(name="market for electricity, medium voltage", location="DE")
# es = bd.Database("cutoff39").get(name="market for electricity, medium voltage", location="ES")
# fr = bd.Database("cutoff39").get(name="market for electricity, medium voltage", location="FR")
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

pass


#__________________________________________________________________________
#Now we join technosphere and bioshpere IA results and export it:
LCScore_all = pd.concat([technoframe,bioframe])

#Re-arrange cas number field position, since currently, LCIA matrices from excel includes less methods than the ones available in brightway
LCScore_all.insert(15, "CAS number", LCScore_all.pop("CAS number"))

#To include units of each impact category, we retrieve it from the implementation file:
# Cast the columns to strings
indicators = pd.read_excel(implementation_file, sheet_name="Indicators")
indicators = indicators.astype(str)
indicators_1 = indicators[['Method', 'Category', 'Indicator']]
indicators_2 = indicators[['Indicator Unit']]

row_list = [' '.join(row) for row in indicators_1.values.tolist()] # Use list comprehension to create a list of comma-separated values for each row
ind_headers_dict = dict(zip(row_list, indicators_2['Indicator Unit'])) # Create a dictionary of headers and units
LCScore_all.columns = ['{}{}'.format(col, ' ({})'.format(ind_headers_dict.get(col)) if col in ind_headers_dict else '') for col in LCScore_all.columns] # Rename the columns using dictionary comprehension

#Export database
LCScore_all.to_csv("LCScore_all.csv", index=False)
LCScore_all.to_excel("LCScore_all.xlsx", index=False)


#We can also export ei+bio info without LCIA scores:
BioTechno_info = pd.concat([ei,bio])
BioTechno_info.to_csv("BioTechno_info.csv", index=False)

#And names from BioTechno matrix only:
BioTechno_names = BioTechno_info[['Activity / Exchange name', 'Reference product / Compartment', 'Location / Subcompartment', 'unit', 'comment']]
BioTechno_names.to_csv("BioTechno_names.csv", index=False)
BioTechno_names.to_excel("BioTechno_names.xlsx", index=False)


#Finally we can export databases:
#technoframe.to_csv("technoframe.csv", index=True)
#bioframe.to_csv("bioframe.csv", index=True)

#We already export those with the original column names, so we don't do it now:
#ei.to_csv("ei.csv", index=True)
#bio.to_csv("bio.csv", index=True)


#If we want to export just LCIA scores from ReCiPe method, (H) perspective, including Long Term (LT) emissions
recipe_methods = [i for i in LCScore_all.columns if 'ReCiPe' in i and '(H)' in i and 'no LT' not in i]
first_cols = list(LCScore_all.iloc[:, :18].columns)
cols_to_drop = [col for col in LCScore_all.columns if col not in recipe_methods and col not in first_cols]
LCScore_recipe = LCScore_all.drop(columns=cols_to_drop)

LCScore_recipe.to_csv("LCScore_recipe.csv", index=False)
LCScore_recipe.to_excel("LCScore_recipe.xlsx", index=False)


#if we want to export CED methods:
CED_methods = [i for i in LCScore_all.columns if 'CED' in i]

#together with ReCiPe:
recipe_methods = [i for i in LCScore_all.columns if 'ReCiPe' in i and '(H)' in i and 'no LT' not in i]
first_cols = list(LCScore_all.iloc[:, :18].columns)
cols_to_drop = [col for col in LCScore_all.columns if col not in recipe_methods and col not in first_cols and col not in CED_methods]
LCScore_recipe_ced = LCScore_all.drop(columns=cols_to_drop)

LCScore_recipe_ced.to_csv("LCScore_recipe_ced.csv", index=False)
LCScore_recipe_ced.to_excel("LCScore_recipe_ced.xlsx", index=False)



pass
