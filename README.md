# Lo_Chairs_vs_Residency_directors

ACGME_all_OBGYN_fellowships - I was able to write code that scraped the ACGME fellowship directors. After that I uploaded the names to Mturk that gave me the NPI numbers.  

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



