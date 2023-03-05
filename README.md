# Lo_Chairs_vs_Residency_directors

ACGME_all_OBGYN_fellowships - I was able to scrape the ACGME fellowship directors. After that I uploaded the names to Mturk that gave me the NPI numbers.  

* Shilpa found the names of the OBGYN and Urology chairs.  From that we were able to use mechanical turk to search their NPI, PPI, and healthgrades.com age.  Lo and Iris pulled the ACGME data by hand for Urology and OBGYN residencies.  

* Most of the data munging was done in exploratory.io in the Lo_and_Muffly project, Chair vs. Residency folder, Compiled Programs from Shilpa.  
* Subspecialty for OBGYN can be found in GOBA.  Subspecialty for Urology can be found in ABU.  But only FPMRS were on the ABU site so we had to look at the Urology Care Foundation for subspecialty as well.  




Cool Tip:
* https://www.alecdibble.com/blog/large-csvs-on-mac/#tldr


A list of OBGYN and Urology chairs was gathered by e-mailing department secretaries, calling OBGYN departments, and searching department web sites to get the chair names.  Military program chairs were eliminated as they were few and far between.  The NPI and PPI of each chair were hand searched.  Finally we ran an inner join between the payments and the PPI.  

This was done using `sqldf` to do inner joins within the RAM limits of R and these large files: `sqldf('select OP_14.* from OP_14 join StudyGroup on OP_14.Physician_Profile_ID = StudyGroup.PPI' )`.  The file was called `3_1_Load_Data.R`.  Each year takes about 20 minutes to run the inner join between the selected PPI numbers and the payments data.  



# For the CHAIR AND PD 
"chair_name_to_PPI_crosswalk_Batch_4702688_batch_results" in exploratory - 196 chair names with PPI numbers.  From mturk search.  This file is the chart of the chair data.  

"payment_PD_Chair_demographics" - 599 chairs, PDs, PD and chairs.  Name matchned and that bourght in the NPI number.  

"chair_name_to_NPI_crosswalk_npi_number_Batch_4702607_batch_results" in exploratory - 296 chair names with NPI numbers.  From mturk search.  
"Batch_4673013_batch_results" - **Not helpful.**  Chair names fo rinternal medicine, etc.  From mturk.  
"chair_age_Batch_4702625_batch_results" - 247 chair names and their ages searched in healthgrades.com.  
"PPI_from_mturk" - Chairs with their PPI numbers.  

# ACGME data
To use Docker in addition to some bespoke code from Bart.  See directory: "/Users/tylermuffly/Dropbox (Personal)/workforce/Master_Scraper"

# Crowdsourcing
All the data is public information and can be shared.  Dates of starting and ending as chair of OBGYN or Urology were found most easily at LinkedIn.  The dates for program directors are ACGME (https://apps.acgme.org/ads/Public/Programs/Search).
```r
https://apps.acgme.org/ads/Public/Programs/Detail?programId=3288&ReturnUrl=https%3A%2F%2Fapps.acgme.org%2Fads%2FPublic%2FPrograms%2FSearch
Director Information
Audra R Williams, MD, MPH
Assistant Professor, Residency Program Director
Director First Appointed:
September 01, 2022
```

# The Open Payments Data
https://www.cms.gov/openpayments/data/dataset-downloads - Before 2021 there was no link between PPI and NPI number.  The 2021 data includes both variables and should be much easier to use.  This is typical amazing work by Joe Guido over the years.  

Download_all_data.R
```r
#Data from CMS is located at:
time.1 <- Sys.time()

decompress_file <- function(directory, file, .file_cache = FALSE) {

  if (.file_cache == TRUE) {
    print("decompression skipped")
  } else {

    # Set working directory for decompression
    # simplifies unzip directory location behavior
    wd <- getwd()
    setwd(directory)

    # Run decompression
    decompression <-
      system2("unzip",
              args = c("-o", # include override flag
                       file),
              stdout = TRUE)

    # uncomment to delete archive once decompressed
    # file.remove(file)

    # Reset working directory
    setwd(wd); rm(wd)

    # Test for success criteria
    # change the search depending on
    # your implementation
    if (grepl("Warning message", tail(decompression, 1))) {
      print(decompression)
    }
  }
}

directory <-"/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/"
data_folder <- ""

# create a temporary file and a temporary directory on your local disk
tf <- tempfile()
td <- tempdir()

#2019
download.file("https://download.cms.gov/openpayments/PGYR19_P063020.ZIP",
              tf,
              mode = "wb")
# unzip the files to the temporary directory
files <- unzip( tf , exdir = td )

# here are your files
files

#decompress_file(directory = directory, "2019_dataset.zip")
dataset_2019 <- read.delim(files[1])
dataset_2019 %<>% mutate(year = "2019")

master <- rbind (dataset_2013, dataset_2014, dataset_2015, dataset_2016, dataset_2017, dataset_2018, dataset_2019)

dim(master)
summary(master)
master$year <- as.factor (master$year)

write.csv(master, paste0(data_folder,"open_payments_master.csv"))
time.2 <- Sys.time()

cat(sprintf(" %.1f", as.numeric(difftime(time.2, time.1,  units="secs"))), " secs\n")
```

# Code block on 'get_data.R':
We can't use 'data.table' given that Mac can't run OpenMP with multiple threads.  Instead we are using 'sqldf' that is slower but more proven and stable. The get_data.R file takes the StudyGroup variable which is two columns: NPI and PPI numbers.  This allows for the identification of the chairs and program directors of interest.   

```r
library(sqldf)
library(sendmailR)
library(twilio)

# Load the StudyGroup data frame
StudyGroup <- read.csv("~/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/Study_Group.csv", header = TRUE)

# Define the years of interest
years <- c(2013)
  
  # Construct the filename for the COI data for this year
  coi_file <- paste0("/Volumes/Video Projects Muffly 1/coi/OP_DTL_GNRL_PGYR2013_P06302020.csv")
  
  coi_data <- readr::read_csv(coi_file) #, header = TRUE)
  
  # Define the SQL query for the inner join
  query <- paste0("SELECT *
                     FROM StudyGroup
                     INNER JOIN coi_data
                     ON StudyGroup.Physician_Profile_ID = coi_data.Physician_Profile_ID")
  
  joined_data_2013 <- sqldf(query)
  
  # Remove the original data frames from memory
  rm(coi_data)
  invisible(gc())
  
  # Save the joined data frame as a CSV file
  write.csv(joined_data_2013, file = "/Volumes/Video Projects Muffly 1/coi/joined_data_2013.csv", row.names = FALSE); rm(joined_data_2013)
```
# Table 1
Built with arsenal from data that I cleaned up in it's own exploratory branch called Table1.  

# Table 2
See exploratory branch called Table 2 under new_financials.  

# Figure 1
Exploratory: figure - cran_figure.  I created a new branch called joined_data_all.  This is exported to 'Lo_cran_releases_adapted.RMD'.  
```r
# Data Wrangling ----------------------------------------------------------
# Data Reading 
cran1 <- readr::read_csv("~/Dropbox (Personal)/Lo_Fellowship_Directors/data/joined_data_all.csv")
```

I can't get the `ggsave` to work so I plotted it and took a screenshot.  


















## Exploratory.io code
### ~chair_name_dates_started
```r
# Set libPaths.
.libPaths("/Users/tylermuffly/.exploratory/R/4.2")

# Load required packages.
library(janitor)
library(lubridate)
library(hms)
library(tidyr)
library(stringr)
library(readr)
library(cpp11)
library(forcats)
library(RcppRoll)
library(dplyr)
library(tibble)
library(bit64)
library(zipangu)
library(exploratory)

# Steps to produce Crosswalk_ACOG_Districts_1
`Crosswalk_ACOG_Districts_1` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/Crosswalk_ACOG_Districts_1.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce chair_PPI_Batch_5014289_batch_results
`chair_PPI_Batch_5014289_batch_results` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/from_mturk/chair_PPI_Batch_5014289_batch_results.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  select(Input.specialty, Input.Program_id_number, Input.Program_Director, Input.first, Input.last, Input.state, Answer.address, Answer.comments, Answer.physician_name, Answer.physician_profile_id, Answer.physician_specialty) %>%
  mutate(Answer.physician_profile_id = case_when(Answer.physician_profile_id %in% c("N/A", "na", "na, no button", "Not Found") ~ as.character(NA) , TRUE ~ as.character(Answer.physician_profile_id))) %>%
  filter(!is.na(Answer.physician_profile_id)) %>%
  distinct(Answer.physician_profile_id, .keep_all = TRUE) %>%
  filter(Answer.physician_profile_id != "1003801929") %>%
  mutate(test = str_count(Answer.physician_profile_id, "\\d")) %>%
  filter(test < 10) %>%
  
  # Only once chair per program
  distinct(Input.Program_id_number, .keep_all = TRUE)

# Steps to produce obgyn_and_uro
`obgyn_and_uro` <- exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/obgyn_and_uro.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  filter((is.na(Specialty) | Specialty != "ENT")) %>%
  mutate(`Program number` = str_extract_all(Program, "[[:digit:]]+")) %>%
  mutate(`Program number` = list_to_text(`Program number`))

# Steps to produce merge_with_hand_search_mutate_7
`merge_with_hand_search_mutate_7` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_mturk/merge with hand search_mutate_7.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  filter(PPI != "No PPI present") %>%
  distinct(PPI, .keep_all = TRUE)

# Steps to produce missing_chair
`missing_chair` <- 
  # All chairs and program directors.  
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_Lo/missing chair.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  separate(chair_first_last, into = c("chair_first", "chair_last"), sep = "\\s+", remove = FALSE, convert = TRUE) %>%
  mutate(`Program number` = as.character(`Program number`)) %>%
  rename(Age = Age.x, Gender = Gender.x, NPI = NPI.x)

# Steps to produce payment_PD_Chair_demographics
`payment_PD_Chair_demographics` <- 
  # One row per person.  EVERY person received a payment.  
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_Fellowship_Directors/data/payment_PD_Chair_distinct_51.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  rename(date = Date_of_Payment) %>%
  rename(Age_range = `Age range`) %>%
  mutate(Age_range = fct_relevel(Age_range, "30-39", "40-49", "50-59", "60-69", "70-79", ">80")) %>%
  
  # One row per NPI.  
  distinct(NPI, .keep_all = TRUE) %>%
  select(-Total_Amount_of_Payment_USDollars, -Number_of_Payments_Included_in_Total_Amount, -Nature_of_Payment_or_Transfer_of_Value, -date, -Date_of_Payment_month, -Program_Year, -Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1, -Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, -Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name) %>%
  mutate(first = humaniformat::first_name(First_Last)) %>%
  mutate(last = humaniformat::last_name(First_Last))

# Steps to produce chair_name_to_PPI_crosswalk_Batch_4702688_batch_results
`chair_name_to_PPI_crosswalk_Batch_4702688_batch_results` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_mturk/chair_PPI_to_name_crosswalk_Batch_4702688_batch_results.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Chicago", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  
  # From mturk.  
  select(Input.Department_Chair, Input.first_last, Input.first, Input.last, Answer.address, Answer.comments, Answer.physician_name, Answer.physician_profile_id, Answer.physician_specialty) %>%
  filter(Answer.physician_specialty %nin% c("BERI M RIDGEWAY M.D", "Dental Providers|Dentist|General Practice", "na", "RAJU THOMAS", "ROBERT SVATEK") & Answer.comments %nin% c("ID NOT FOUND", "no results", "No results found for physicians that meet your search criteria", "No results found for physicians that meet your search criteria.", "No results found for physicians that meet your search criteria.Refine", "NOT FOUND")) %>%
  select(-Answer.comments, -Answer.physician_specialty) %>%
  filter(!is.na(Answer.physician_profile_id) & Answer.physician_profile_id %nin% c("na", "n a", "N/A", "125 PATERSON ST CLINICAL ACADEMIC BUILDING - SUITE 4100 NEW BRUNSWICK, NJ 08901-1962", "1437276938", "50 N MEDICAL DR SALT LAKE CITY, UT 84132-0001")) %>%
  mutate(Answer.physician_name = str_to_title(Answer.physician_name), Answer.physician_name = str_remove(Answer.physician_name, regex("\\\\t", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  select(-Input.first_last, -Answer.address, -Answer.physician_name) %>%
  distinct(Answer.physician_profile_id, .keep_all = TRUE)

# Steps to produce ~chair_name_dates_started
`~chair_name_dates_started` <- 
  # This is where the data came from:  https://docs.google.com/spreadsheets/d/1aaTldmVRuPOeNxfvSO3YlSf8FPFr474My7dHfSEs630/edit#gid=957335077
  # 
  # 
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/from_mturk/chair_name_to_PPI_crosswalk_Batch_4702688_batch_results_1 - chair_name_to_PPI_crosswalk_Batch_4702688_batch_results_1.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(`Chair started year` = case_when(`Chair started year` == "na" ~ as.character(NA) , TRUE ~ as.character(`Chair started year`))) %>%
  
  # Pulls out year only
  mutate(`Chair started year` = str_remove_all(`Chair started year`, "[:alpha:]+")) %>%
  mutate(`Chair started year` = str_clean(`Chair started year`)) %>%
  
  # Pull out the year.  
  dplyr::mutate(year = stringr::str_extract(`Chair started year`, "\\d{4}")) %>%
  
  # Inputes the year for missing.  
  mutate(year = impute_na(year, type = "value", val = "2012")) %>%
  select(-`Chair started year`, -`Website 1`, -`Website 2`) %>%
  bind_rows(`chair_name_to_PPI_crosswalk_Batch_4702688_batch_results`, id_column_name = "ID", current_df_name = "~chair_name_dates_started", force_data_type = FALSE) %>%
  left_join(`payment_PD_Chair_demographics`, by = c("Input.first" = "first", "Input.last" = "last")) %>%
  rename(`Year the Chair Started` = year) %>%
  select(Input.Specialty, Input.first_last, `Year the Chair Started`, Input.Program) %>%
  
  # Only looking at chair jobs.  
  mutate(Position = "Chair") %>%
  rename(Program = Input.Program, `Program Director` = Input.first_last, Specialty = Input.Specialty, `Director Date Appointed` = `Year the Chair Started`) %>%
  
  # Pull out the program Id so it can be matched to the residency program of interest.  
  mutate(Program_id_number = list_to_text(str_extract_inside(Program, "[","]", include_special_chars = FALSE, all = TRUE)), .after = ifelse("Program" %in% names(.), "Program", last_col())) %>%
  reorder_cols(Specialty, Program, Program_id_number, `Director Date Appointed`, `Positions approved`, `Positions filled`, Address, `Program Director`, `Accredidation status`, Position) %>%
  mutate(`Director Date Appointed` = parse_number(`Director Date Appointed`)) %>%
  
  # Clean up 1201 typographical error.  
  mutate(`Director Date Appointed` = recode(`Director Date Appointed`, `1201` = 2011)) %>%
  
  # Arrange by name of the chair.  
  arrange(`Program Director`) %>%
  
  # Brings in the NPI of the chair with match by specialty, program id number and name.  
  left_join(`missing_chair`, by = c("Program Director" = "chair_first_last", "Specialty" = "Specialty.x", "Program_id_number" = "Program number"), target_columns = c("chair_first_last", "Specialty.x", "Program number", "NPI"), ignorecase=TRUE) %>%
  
  # Find the PPI of all the NPI numbers.  
  left_join(`merge_with_hand_search_mutate_7`, by = c("NPI" = "NPI", "Position" = "Position.x"), target_columns = c("NPI", "Position.x", "Position.x", "PPI")) %>%
  rename(Physician_Profile_ID = PPI) %>%
  
  # Sent to mturk for ppi search
  mutate(Physician_Profile_ID = parse_number(Physician_Profile_ID)) %>%
  
  # Bring in data that will be available when bind rows is present.  
  left_join(`obgyn_and_uro`, by = c("Program_id_number" = "Program number"), target_columns = c("Program number", "Positions approved", "Positions filled", "Address", "Accredidation status", "Age", "Gender")) %>%
  mutate(first = humaniformat::first_name(`Program Director`)) %>%
  mutate(last = humaniformat::last_name(`Program Director`)) %>%
  
  # Bring in hand search mturk PPI values.  
  left_join(`chair_PPI_Batch_5014289_batch_results`, by = c("first" = "Input.first", "last" = "Input.last"), target_columns = c("Input.first", "Input.last", "Answer.physician_profile_id"), ignorecase=TRUE) %>%
  
  # Should only be one chair per program.  
  distinct(Program_id_number, .keep_all = TRUE) %>%
  
  # Make it so that one person can't be the chair in more than one department at a time.  
  distinct(`Program Director`, .keep_all = TRUE) %>%
  mutate(Answer.physician_profile_id = parse_number(Answer.physician_profile_id), Physician_Profile_ID = coalesce(Physician_Profile_ID, Answer.physician_profile_id)) %>%
  
  # After the hand-searched and old PPI numbers are coalesced then we remove the hand-searched PPI column.  
  select(-Answer.physician_profile_id) %>%
  
  # Chairs who did and did not receive pyaments.  
  mutate(Received_payment = Physician_Profile_ID, .after = ifelse("Physician_Profile_ID" %in% names(.), "Physician_Profile_ID", last_col())) %>%
  mutate(Received_payment = ifelse(is.na(Received_payment), "Did NOT receive payments", "Received payment")) %>%
  
  # Only one person per department and only one NPI per person.  
  distinct(NPI, .keep_all = TRUE) %>%
  reorder_cols(Specialty, Program, Program_id_number, `Director Date Appointed`, `Positions approved`, `Positions filled`, Address, `Program Director`, `Accredidation status`, Age, Gender, NPI, Physician_Profile_ID, Received_payment, first, last, Position) %>%
  mutate(honorrific = "MD")

# Steps to produce OP_Matched_smaller_imp_1
`OP_Matched_smaller_imp_1` <- exploratory::read_delim_file("../committed_data/OP_Matched_smaller.csv" , ",", quote = "\"", skip = 0 , col_names = TRUE , na = c('','NA') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>% `_tam_isDataFrame`() %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce missing_PPI_s_imp_1
`missing_PPI_s_imp_1` <- exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/missing_PPI_s_imp_1.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce ~Residency_PDs_and_Chairs
`~Residency_PDs_and_Chairs` <- 
  # Residency program directors.  
  exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Compiled_Programs_1.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(`Director Date Appointed` = excel_numeric_to_date(`Director Date Appointed`)) %>%
  mutate(`Director Date Appointed` = year(`Director Date Appointed`)) %>%
  filter(Specialty %in% c("OBGYN", "Urology")) %>%
  
  # Match the NPI to NPI with PPI.  
  left_join(missing_PPI_s_imp_1, by = c("NPI" = "Input.NPI")) %>%
  
  # Match NPI to NPI and see the PPI results.  
  left_join(`OP_Matched_smaller_imp_1`, by = c("NPI" = "NPI")) %>%
  select(-Input.name, -Physician_Profile_First_Name, -Physician_Profile_Middle_Name, -Physician_Profile_Last_Name, -Physician_Profile_Suffix, -Physician_Profile_Alternate_First_Name, -Physician_Profile_Alternate_Middle_Name) %>%
  mutate(`Physician PPI` = parse_number(`Physician PPI`)) %>%
  
  # Must have a PPI number to receive payments.  So the people who do not have a PPI never received payments.  
  mutate(Physician_Profile_ID = coalesce(Physician_Profile_ID,`Physician PPI`)) %>%
  select(-`Physician PPI`) %>%
  mutate(first = humaniformat::first_name(`Program Director`)) %>%
  separate(`Program Director`, into = c("name", "honorrific"), sep = "\\s*\\,\\s*", remove = FALSE, convert = TRUE) %>%
  mutate(last = humaniformat::last_name(`name`)) %>%
  mutate(Received_payment = Physician_Profile_ID, .after = ifelse("Physician_Profile_ID" %in% names(.), "Physician_Profile_ID", last_col())) %>%
  mutate(Received_payment = ifelse(is.na(Received_payment), "Did NOT receive payments", "Received payment")) %>%
  
  # Only one program director NPI.  
  distinct(`Program Director`, .keep_all = TRUE) %>%
  mutate(Program_id_number = list_to_text(str_extract_inside(Program, "[","]", include_special_chars = FALSE, all = TRUE)), .after = ifelse("Program" %in% names(.), "Program", last_col())) %>%
  
  # Pulls out the residency_id
  distinct(Program_id_number, .keep_all = TRUE) %>%
  
  # Create a Position Column.  
  mutate(Position = "Resideny Program Director") %>%
  select(-name) %>%
  
  # Brind Program Directors and Chairs together into one document.  UNION BRINGS RESULTS FROM TWO TABLES AND EXCLUDES DUPLICATES.  Baller!
  bind_rows(`~chair_name_dates_started`, id_column_name = "ID", current_df_name = "~Residency_PDs_and_Chairs", force_data_type = TRUE) %>%
  mutate(ID = recode(ID, "~chair_name_dates_started" = "Chair")) %>%
  rename(`Program Director or Chair` = `Program Director`) %>%
  mutate(state = (str_extract(Address, "\\b[A-Z]{2}\\b"))) %>%
  mutate(state = statecode(state, output_type = "name")) %>%
  left_join(`Crosswalk_ACOG_Districts_1`, by = c("state" = "State"), target_columns = c("State", "ACOG_District")) %>%
  mutate(ACOG_District = coalesce(ACOG_District, state)) %>%
  mutate(ACOG_District = recode(ACOG_District, "Delaware" = "District III", "Maine" = "District I")) %>%
  select(-honorrific) %>%
  mutate(Years_in_position = 2022-`Director Date Appointed`) %>%
  select(-ID) %>%
  distinct(NPI, .keep_all = TRUE)

# Steps to produce Missing_Chair_Info
`Missing_Chair_Info` <- 
  # Lo sent me this google sheet.  
  exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_Lo/Missing Chair Info.xlsx", sheet = 1, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(`Chair name` = case_when(`Chair name` == "N/A" ~ as.character(NA) , TRUE ~ `Chair name`)) %>%
  filter(!is.na(`Chair name`)) %>%
  filter(PPI %nin% c("no PPI", "noPPI")) %>%
  mutate(PPI = parse_number(PPI)) %>%
  filter(!is.na(PPI)) %>%
  select(NPI, PPI)

# Steps to produce hand_search_needed_of_ppi_NPI_to_PPI_crosswalk_1_join_6
`hand_search_needed_of_ppi_NPI_to_PPI_crosswalk_1_join_6` <- exploratory::select_columns(exploratory::clean_data_frame(exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_mturk/hand_search_needed_of_ppi_NPI_to_PPI_crosswalk_1_join_6.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE)),"NPI","Name","Position","first","last","PPI") %>%
  readr::type_convert()

# Steps to produce branching_point_1
`branching_point_1` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_Lo/NPI to PPI crosswalk.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "ISO-8859-1", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce merge with hand search
`merge with hand search` <- `branching_point_1` %>%
  mutate(NPI = parse_number(NPI)) %>%
  left_join(`hand_search_needed_of_ppi_NPI_to_PPI_crosswalk_1_join_6`, by = c("Name" = "Name")) %>%
  mutate_at(vars(Answer.physician_profile_id, PPI), funs(as.character)) %>%
  mutate(Answer.physician_profile_id = coalesce(Answer.physician_profile_id, PPI)) %>%
  select(-NPI.y, -Position.y, -first, -last, -PPI) %>%
  mutate(Answer.physician_profile_id = impute_na(Answer.physician_profile_id, type = "value", val = "No PPI present"))

# Steps to produce the output
`branching_point_1` %>%
  filter(is.na(Answer.physician_profile_id)) %>%
  mutate(first = word(Name, 1, sep = "\\s+"), last = word(Name, -1, sep = "\\s+")) %>%
  select(-Answer.physician_profile_id) %>%
  mutate(NPI_number = as.numeric(NPI)) %>%
  distinct(NPI_number, .keep_all = TRUE) %>%
  distinct(NPI_number, .keep_all = TRUE) %>%
  bind_rows(`merge with hand search`, id_column_name = "ID", current_df_name = "~NPI_to_PPI_crosswalk", force_data_type = TRUE) %>%
  bind_rows(`hand_search_needed_of_ppi_NPI_to_PPI_crosswalk_1_join_6`, id_column_name = "ID", current_df_name = "~NPI_to_PPI_crosswalk", force_data_type = FALSE) %>%
  bind_rows(`merge_with_hand_search_mutate_7`, id_column_name = "ID", current_df_name = "~NPI_to_PPI_crosswalk", force_data_type = FALSE) %>%
  mutate(across(c(Answer.physician_profile_id, PPI), parse_number)) %>%
  bind_rows(`Missing_Chair_Info`, id_column_name = "ID", current_df_name = "~NPI_to_PPI_crosswalk", force_data_type = FALSE) %>%
  mutate(NPI = coalesce(NPI, NPI_number, NPI.x)) %>%
  select(-NPI_number, -NPI.x) %>%
  mutate(PPI = coalesce(PPI, Answer.physician_profile_id)) %>%
  select(-ID.new.new.new, -ID.new.new, -ID.new, -ID, -Answer.physician_profile_id) %>%
  mutate(Position = coalesce(Position, Position.x)) %>%
  select(-Position.x) %>%
  arrange(Name) %>%
  distinct(NPI, .keep_all = TRUE) %>%
  distinct(Name, .keep_all = TRUE) %>%
  select(-first, -last) %>%
  mutate(last = humaniformat::last_name(Name)) %>%
  mutate(first = humaniformat::first_name(Name)) %>%
  left_join(`chair_name_to_PPI_crosswalk_Batch_4702688_batch_results`, by = c("first" = "Input.first", "last" = "Input.last"), target_columns = c("Input.first", "Input.last", "Answer.physician_profile_id"), ignorecase=TRUE) %>%
  mutate(Answer.physician_profile_id = parse_number(Answer.physician_profile_id)) %>%
  mutate(PPI = coalesce(PPI, Answer.physician_profile_id)) %>%
  select(-Answer.physician_profile_id) %>%
  distinct(NPI, .keep_all = TRUE) %>%
  full_join(`~chair_name_dates_started`, by = c("NPI" = "NPI"), target_columns = c("NPI", "Physician_Profile_ID")) %>%
  mutate(PPI = coalesce(PPI, Physician_Profile_ID)) %>%
  select(-Physician_Profile_ID) %>%
  left_join(`~Residency_PDs_and_Chairs`, by = c("NPI" = "NPI"), target_columns = c("NPI", "Physician_Profile_ID")) %>%
  mutate(PPI = coalesce(PPI, Physician_Profile_ID)) %>%
  select(-Name, -Position, -last, -first, -Physician_Profile_ID) %>%
  
  # Input as Study_Group variable to the get_data.R file.  
  distinct(NPI, .keep_all = TRUE) %>%
  rename(Physician_Profile_ID = PPI)
```

### ~Residency and Chairs
```r
# Set libPaths.
.libPaths("/Users/tylermuffly/.exploratory/R/4.2")

# Load required packages.
library(janitor)
library(lubridate)
library(hms)
library(tidyr)
library(stringr)
library(readr)
library(cpp11)
library(forcats)
library(RcppRoll)
library(dplyr)
library(tibble)
library(bit64)
library(zipangu)
library(exploratory)

# Steps to produce Crosswalk_ACOG_Districts_1
`Crosswalk_ACOG_Districts_1` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/Crosswalk_ACOG_Districts_1.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce chair_PPI_Batch_5014289_batch_results
`chair_PPI_Batch_5014289_batch_results` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/from_mturk/chair_PPI_Batch_5014289_batch_results.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  select(Input.specialty, Input.Program_id_number, Input.Program_Director, Input.first, Input.last, Input.state, Answer.address, Answer.comments, Answer.physician_name, Answer.physician_profile_id, Answer.physician_specialty) %>%
  mutate(Answer.physician_profile_id = case_when(Answer.physician_profile_id %in% c("N/A", "na", "na, no button", "Not Found") ~ as.character(NA) , TRUE ~ as.character(Answer.physician_profile_id))) %>%
  filter(!is.na(Answer.physician_profile_id)) %>%
  distinct(Answer.physician_profile_id, .keep_all = TRUE) %>%
  filter(Answer.physician_profile_id != "1003801929") %>%
  mutate(test = str_count(Answer.physician_profile_id, "\\d")) %>%
  filter(test < 10) %>%
  
  # Only once chair per program
  distinct(Input.Program_id_number, .keep_all = TRUE)

# Steps to produce obgyn_and_uro
`obgyn_and_uro` <- exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/obgyn_and_uro.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  filter((is.na(Specialty) | Specialty != "ENT")) %>%
  mutate(`Program number` = str_extract_all(Program, "[[:digit:]]+")) %>%
  mutate(`Program number` = list_to_text(`Program number`))

# Steps to produce merge_with_hand_search_mutate_7
`merge_with_hand_search_mutate_7` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_mturk/merge with hand search_mutate_7.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  filter(PPI != "No PPI present") %>%
  distinct(PPI, .keep_all = TRUE)

# Steps to produce missing_chair
`missing_chair` <- 
  # All chairs and program directors.  
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_Lo/missing chair.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  separate(chair_first_last, into = c("chair_first", "chair_last"), sep = "\\s+", remove = FALSE, convert = TRUE) %>%
  mutate(`Program number` = as.character(`Program number`)) %>%
  rename(Age = Age.x, Gender = Gender.x, NPI = NPI.x)

# Steps to produce payment_PD_Chair_demographics
`payment_PD_Chair_demographics` <- 
  # One row per person.  EVERY person received a payment.  
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_Fellowship_Directors/data/payment_PD_Chair_distinct_51.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  rename(date = Date_of_Payment) %>%
  rename(Age_range = `Age range`) %>%
  mutate(Age_range = fct_relevel(Age_range, "30-39", "40-49", "50-59", "60-69", "70-79", ">80")) %>%
  
  # One row per NPI.  
  distinct(NPI, .keep_all = TRUE) %>%
  select(-Total_Amount_of_Payment_USDollars, -Number_of_Payments_Included_in_Total_Amount, -Nature_of_Payment_or_Transfer_of_Value, -date, -Date_of_Payment_month, -Program_Year, -Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1, -Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, -Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name) %>%
  mutate(first = humaniformat::first_name(First_Last)) %>%
  mutate(last = humaniformat::last_name(First_Last))

# Steps to produce chair_name_to_PPI_crosswalk_Batch_4702688_batch_results
`chair_name_to_PPI_crosswalk_Batch_4702688_batch_results` <- exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/from_mturk/chair_PPI_to_name_crosswalk_Batch_4702688_batch_results.csv", delim = ",", quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Chicago", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  
  # From mturk.  
  select(Input.Department_Chair, Input.first_last, Input.first, Input.last, Answer.address, Answer.comments, Answer.physician_name, Answer.physician_profile_id, Answer.physician_specialty) %>%
  filter(Answer.physician_specialty %nin% c("BERI M RIDGEWAY M.D", "Dental Providers|Dentist|General Practice", "na", "RAJU THOMAS", "ROBERT SVATEK") & Answer.comments %nin% c("ID NOT FOUND", "no results", "No results found for physicians that meet your search criteria", "No results found for physicians that meet your search criteria.", "No results found for physicians that meet your search criteria.Refine", "NOT FOUND")) %>%
  select(-Answer.comments, -Answer.physician_specialty) %>%
  filter(!is.na(Answer.physician_profile_id) & Answer.physician_profile_id %nin% c("na", "n a", "N/A", "125 PATERSON ST CLINICAL ACADEMIC BUILDING - SUITE 4100 NEW BRUNSWICK, NJ 08901-1962", "1437276938", "50 N MEDICAL DR SALT LAKE CITY, UT 84132-0001")) %>%
  mutate(Answer.physician_name = str_to_title(Answer.physician_name), Answer.physician_name = str_remove(Answer.physician_name, regex("\\\\t", ignore_case = TRUE), remove_extra_space = TRUE)) %>%
  select(-Input.first_last, -Answer.address, -Answer.physician_name) %>%
  distinct(Answer.physician_profile_id, .keep_all = TRUE)

# Steps to produce ~chair_name_dates_started
`~chair_name_dates_started` <- 
  # This is where the data came from:  https://docs.google.com/spreadsheets/d/1aaTldmVRuPOeNxfvSO3YlSf8FPFr474My7dHfSEs630/edit#gid=957335077
  # 
  # 
  exploratory::read_delim_file("/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/from_mturk/chair_name_to_PPI_crosswalk_Batch_4702688_batch_results_1 - chair_name_to_PPI_crosswalk_Batch_4702688_batch_results_1.csv", delim = NULL, quote = "\"" , col_names = TRUE , na = c('') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(`Chair started year` = case_when(`Chair started year` == "na" ~ as.character(NA) , TRUE ~ as.character(`Chair started year`))) %>%
  
  # Pulls out year only
  mutate(`Chair started year` = str_remove_all(`Chair started year`, "[:alpha:]+")) %>%
  mutate(`Chair started year` = str_clean(`Chair started year`)) %>%
  
  # Pull out the year.  
  dplyr::mutate(year = stringr::str_extract(`Chair started year`, "\\d{4}")) %>%
  
  # Inputes the year for missing.  
  mutate(year = impute_na(year, type = "value", val = "2012")) %>%
  select(-`Chair started year`, -`Website 1`, -`Website 2`) %>%
  bind_rows(`chair_name_to_PPI_crosswalk_Batch_4702688_batch_results`, id_column_name = "ID", current_df_name = "~chair_name_dates_started", force_data_type = FALSE) %>%
  left_join(`payment_PD_Chair_demographics`, by = c("Input.first" = "first", "Input.last" = "last")) %>%
  rename(`Year the Chair Started` = year) %>%
  select(Input.Specialty, Input.first_last, `Year the Chair Started`, Input.Program) %>%
  
  # Only looking at chair jobs.  
  mutate(Position = "Chair") %>%
  rename(Program = Input.Program, `Program Director` = Input.first_last, Specialty = Input.Specialty, `Director Date Appointed` = `Year the Chair Started`) %>%
  
  # Pull out the program Id so it can be matched to the residency program of interest.  
  mutate(Program_id_number = list_to_text(str_extract_inside(Program, "[","]", include_special_chars = FALSE, all = TRUE)), .after = ifelse("Program" %in% names(.), "Program", last_col())) %>%
  reorder_cols(Specialty, Program, Program_id_number, `Director Date Appointed`, `Positions approved`, `Positions filled`, Address, `Program Director`, `Accredidation status`, Position) %>%
  mutate(`Director Date Appointed` = parse_number(`Director Date Appointed`)) %>%
  
  # Clean up 1201 typographical error.  
  mutate(`Director Date Appointed` = recode(`Director Date Appointed`, `1201` = 2011)) %>%
  
  # Arrange by name of the chair.  
  arrange(`Program Director`) %>%
  
  # Brings in the NPI of the chair with match by specialty, program id number and name.  
  left_join(`missing_chair`, by = c("Program Director" = "chair_first_last", "Specialty" = "Specialty.x", "Program_id_number" = "Program number"), target_columns = c("chair_first_last", "Specialty.x", "Program number", "NPI"), ignorecase=TRUE) %>%
  
  # Find the PPI of all the NPI numbers.  
  left_join(`merge_with_hand_search_mutate_7`, by = c("NPI" = "NPI", "Position" = "Position.x"), target_columns = c("NPI", "Position.x", "Position.x", "PPI")) %>%
  rename(Physician_Profile_ID = PPI) %>%
  
  # Sent to mturk for ppi search
  mutate(Physician_Profile_ID = parse_number(Physician_Profile_ID)) %>%
  
  # Bring in data that will be available when bind rows is present.  
  left_join(`obgyn_and_uro`, by = c("Program_id_number" = "Program number"), target_columns = c("Program number", "Positions approved", "Positions filled", "Address", "Accredidation status", "Age", "Gender")) %>%
  mutate(first = humaniformat::first_name(`Program Director`)) %>%
  mutate(last = humaniformat::last_name(`Program Director`)) %>%
  
  # Bring in hand search mturk PPI values.  
  left_join(`chair_PPI_Batch_5014289_batch_results`, by = c("first" = "Input.first", "last" = "Input.last"), target_columns = c("Input.first", "Input.last", "Answer.physician_profile_id"), ignorecase=TRUE) %>%
  
  # Should only be one chair per program.  
  distinct(Program_id_number, .keep_all = TRUE) %>%
  
  # Make it so that one person can't be the chair in more than one department at a time.  
  distinct(`Program Director`, .keep_all = TRUE) %>%
  mutate(Answer.physician_profile_id = parse_number(Answer.physician_profile_id), Physician_Profile_ID = coalesce(Physician_Profile_ID, Answer.physician_profile_id)) %>%
  
  # After the hand-searched and old PPI numbers are coalesced then we remove the hand-searched PPI column.  
  select(-Answer.physician_profile_id) %>%
  
  # Chairs who did and did not receive pyaments.  
  mutate(Received_payment = Physician_Profile_ID, .after = ifelse("Physician_Profile_ID" %in% names(.), "Physician_Profile_ID", last_col())) %>%
  mutate(Received_payment = ifelse(is.na(Received_payment), "Did NOT receive payments", "Received payment")) %>%
  
  # Only one person per department and only one NPI per person.  
  distinct(NPI, .keep_all = TRUE) %>%
  reorder_cols(Specialty, Program, Program_id_number, `Director Date Appointed`, `Positions approved`, `Positions filled`, Address, `Program Director`, `Accredidation status`, Age, Gender, NPI, Physician_Profile_ID, Received_payment, first, last, Position) %>%
  mutate(honorrific = "MD")

# Steps to produce OP_Matched_smaller_imp_1
`OP_Matched_smaller_imp_1` <- exploratory::read_delim_file("../committed_data/OP_Matched_smaller.csv" , ",", quote = "\"", skip = 0 , col_names = TRUE , na = c('','NA') , locale=readr::locale(encoding = "UTF-8", decimal_mark = ".", tz = "America/Denver", grouping_mark = "," ), trim_ws = TRUE , progress = FALSE) %>% `_tam_isDataFrame`() %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce missing_PPI_s_imp_1
`missing_PPI_s_imp_1` <- exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Data/missing_PPI_s_imp_1.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame()

# Steps to produce the output

  # Residency program directors.  
  exploratory::read_excel_file( "/Users/tylermuffly/Dropbox (Personal)/Lo_and_Muffly/Chairs vs. Residency directors/Compiled_Programs_1.xlsx", sheet = 1, skip=0, col_names=TRUE, trim_ws=TRUE, tzone='America/Denver') %>%
  readr::type_convert() %>%
  exploratory::clean_data_frame() %>%
  mutate(`Director Date Appointed` = excel_numeric_to_date(`Director Date Appointed`)) %>%
  mutate(`Director Date Appointed` = year(`Director Date Appointed`)) %>%
  filter(Specialty %in% c("OBGYN", "Urology")) %>%
  
  # Match the NPI to NPI with PPI.  
  left_join(missing_PPI_s_imp_1, by = c("NPI" = "Input.NPI")) %>%
  
  # Match NPI to NPI and see the PPI results.  
  left_join(`OP_Matched_smaller_imp_1`, by = c("NPI" = "NPI")) %>%
  select(-Input.name, -Physician_Profile_First_Name, -Physician_Profile_Middle_Name, -Physician_Profile_Last_Name, -Physician_Profile_Suffix, -Physician_Profile_Alternate_First_Name, -Physician_Profile_Alternate_Middle_Name) %>%
  mutate(`Physician PPI` = parse_number(`Physician PPI`)) %>%
  
  # Must have a PPI number to receive payments.  So the people who do not have a PPI never received payments.  
  mutate(Physician_Profile_ID = coalesce(Physician_Profile_ID,`Physician PPI`)) %>%
  select(-`Physician PPI`) %>%
  mutate(first = humaniformat::first_name(`Program Director`)) %>%
  separate(`Program Director`, into = c("name", "honorrific"), sep = "\\s*\\,\\s*", remove = FALSE, convert = TRUE) %>%
  mutate(last = humaniformat::last_name(`name`)) %>%
  mutate(Received_payment = Physician_Profile_ID, .after = ifelse("Physician_Profile_ID" %in% names(.), "Physician_Profile_ID", last_col())) %>%
  mutate(Received_payment = ifelse(is.na(Received_payment), "Did NOT receive payments", "Received payment")) %>%
  
  # Only one program director NPI.  
  distinct(`Program Director`, .keep_all = TRUE) %>%
  mutate(Program_id_number = list_to_text(str_extract_inside(Program, "[","]", include_special_chars = FALSE, all = TRUE)), .after = ifelse("Program" %in% names(.), "Program", last_col())) %>%
  
  # Pulls out the residency_id
  distinct(Program_id_number, .keep_all = TRUE) %>%
  
  # Create a Position Column.  
  mutate(Position = "Resideny Program Director") %>%
  select(-name) %>%
  
  # Brind Program Directors and Chairs together into one document.  UNION BRINGS RESULTS FROM TWO TABLES AND EXCLUDES DUPLICATES.  Baller!
  bind_rows(`~chair_name_dates_started`, id_column_name = "ID", current_df_name = "~Residency_PDs_and_Chairs", force_data_type = TRUE) %>%
  mutate(ID = recode(ID, "~chair_name_dates_started" = "Chair")) %>%
  rename(`Program Director or Chair` = `Program Director`) %>%
  mutate(state = (str_extract(Address, "\\b[A-Z]{2}\\b"))) %>%
  mutate(state = statecode(state, output_type = "name")) %>%
  left_join(`Crosswalk_ACOG_Districts_1`, by = c("state" = "State"), target_columns = c("State", "ACOG_District")) %>%
  mutate(ACOG_District = coalesce(ACOG_District, state)) %>%
  mutate(ACOG_District = recode(ACOG_District, "Delaware" = "District III", "Maine" = "District I")) %>%
  select(-honorrific) %>%
  mutate(Years_in_position = 2022-`Director Date Appointed`) %>%
  select(-ID) %>%
  distinct(NPI, .keep_all = TRUE)
```
