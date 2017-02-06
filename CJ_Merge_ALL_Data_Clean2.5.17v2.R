############################### TRAINING DATA SET ###########################################
rm(list=ls())
library(lars); library(caret); library(e1071); 
library(class);library(leaps); library(Hmisc); library(plyr); library(devtools); library(ggplot2); # Data visualization
library(readr); # CSV file I/O, e.g. the read_csv function; 
library(lubridate); library(date) # date functions
library(data.table); # Using data.table to speed up runtime
library(repr); # Use repr to adjust plot size
library(profvis); library(dplyr); library(ggmap); library(knitr); library(arules); library(scales) #for scaling dates
library(reshape2); library(bit64); library(showtext);library(zoo); library(dummy);library(tidyr); library(mice); library(stringr)
require(data.table)
library(doParallel); registerDoParallel(cores = 4)
memory.limit(size=60000)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

# load the data
gat.tr <- fread("~/Northwestern University/PREDICT 498/DATA/gender_age_train.csv", head=T, colClasses = c("character", "character", "integer", "character"))# load the "gender, age training data" file
names(gat.tr) #"device_id" "gender"    "age"       "group" 

setkeyv(gat.tr, "device_id")

dim(gat.tr) # 74645     4
summary(gat.tr) #device_id  gender  age  group

set.seed(1306) # set random number generator seed to enable
# repeatability of results
n <- dim(gat.tr)[1]
tr.val.test <- sample(n, round(n/3)) # randomly sample 33% train, validate, and test
ga.train <- gat.tr[tr.val.test,]
ga.train.val <- gat.tr[-tr.val.test,]
vt <- dim(ga.train.val)[1]
val.train <- sample(vt, round(vt/2)) # randomly sample remaining 66% into validate, and test
ga.val <- ga.train.val [-val.train,]
ga.test <- ga.train.val [val.train,]

dim(ga.train) #[1] 24882     4
summary(ga.train)
dim(ga.val) #[1] 24881     4
summary(ga.val)
dim(ga.test) #[1] 24882     4
summary(ga.test)
rm(ga.train.val, ga.val, gat.tr)
#Reloading third file brand Device Type data and setting device ID as the key to create test dataset
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186722      3
setkey(brand,device_id)

#Loading second file brand Device Type data and setting device ID as the key
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3
brand$phone_brand <- as.factor(brand$phone_brand)
brand$device_model <- as.factor(brand$device_model)
brand$phone_brand <- collapsecategory(brand$phone_brand, p = .01)
brand$device_model <- collapsecategory(brand$device_model, p = .01)

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186721      3
setkey(brand,device_id)
summary(brand)
brand.train <- merge(ga.train,brand,by="device_id")


#Remove old object to save memory **Very important with dataset this large**
rm(ga.train)
head(brand.train)
dim(brand.train) # 24883     6
head(brand.train)
summary(brand.train)

#Add event data using device_id as key
events <- read.csv("~/Northwestern University/PREDICT 498/DATA/events.csv", header=T, numerals='no.loss') # load the "mobile events" file 
names(events) # "event_id"  "device_id" "timestamp" "longitude" "latitude" 

dim(events) #3252950       5
#Remove duplicates: None found
#setkey(events,NULL)
#events <-unique(events)
#dim(events) #[1] 3252950       5
events$timestamp <- as.POSIXct((events$timestamp), format=c('%Y-%m-%d %H:%M:%S'))
events$event_id <- as.character(events$event_id)
events$device_id <- as.character(events$device_id)
events <- data.table(events)
events$latitude <- round(events$latitude)
events$longitude <- round(events$longitude)
summary(events)
setkey(events,device_id)

dim(events) #3252950, 5


gat_br_events <- merge(events, brand.train, by="device_id")

#Remove old objects
rm(brand.train,events)
dim(gat_br_events) #399496     10
summary(gat_br_events)
gat_br_events <- data.table(gat_br_events) # device_id event_id timestamp longitude latitude gender age  group phone_brand device_model

#Change key to event ID to merge with app events
setkey(gat_br_events,event_id)


#Add app events and merge to new dataset
app_events <- read.csv("~/Northwestern University/PREDICT 498/DATA/app_events.csv", colClasses = c("character", "character","numeric", "numeric"))
# load the "application events" file and remove "is_installed" since all ==1
app_events<-app_events[,c(1,2,4)]
app_events$event_id <- as.character(app_events$event_id)
names(app_events) #"event_id"     "app_id"   "is_active" 
app_events<- data.table(app_events)
setkey(app_events,event_id) #
dim(app_events) # 32473067        3

mobile2 <- merge(app_events,gat_br_events,by="event_id")

rm(gat_br_events, app_events)
mobile2 <- data.table(mobile2)
summary(mobile2)
dim(mobile2) # 4104969      12
head(mobile2)

#Change key to app_id to merge app_label data
setkey(mobile2,app_id)
head(mobile2)

##Load app label data and merge with label categories before merging with mobile2
app_labels <- fread("~/Northwestern University/PREDICT 498/DATA/app_labels.csv", colClasses = c("character", "character")) # load the "app labels" file
names(app_labels) #"app_id"   "label_id"
app_labels <- data.table(app_labels)
app_labels$label_id <- as.factor(app_labels$label_id)
setkey(app_labels,label_id) #
dim(app_labels) #459943     22
summary(app_labels)

#Load Label Cateogries
label_cat <- read.csv("~/Northwestern University/PREDICT 498/DATA/label_categories_big2.csv", colClasses = c("character", "character")) # load the "label categories" file
names(label_cat) #"label_id" "category" "big_category"
label_cat <- data.table(label_cat)
setkey(label_cat,label_id)
dim(label_cat) #930   3

#Find and remove duplicate entries
dim(label_cat) #
setkey(label_cat, NULL)
labels <- unique(label_cat)
dim(label_cat) #

label_cat$label_id <- as.factor(label_cat$label_id)

summary(label_cat) 
label_cat <- data.table(label_cat)
labels <- merge(label_cat,app_labels,by="label_id", allow.cartesian=TRUE)
labels$flag_1=ifelse(labels$label_id== 773, 1, 0)#Creates a flag for 773,High Flow,flag_1
labels$label_id = ifelse(labels$label_id== 773, NULL,labels$label_id)
labels$flag_3=ifelse(labels$label_id== 188, 1, 0)#Creates a flag for 188, Flight, which has high number of females 24-26 years old,flag_3
labels$label_id = ifelse(labels$label_id== 188, NULL,labels$label_id)
labels$flag_5=ifelse(labels$label_id== 775, 1, 0)#Creates a flag for 775, Liquid Medium, which has high number of older people,flag_5
labels$label_id = ifelse(labels$label_id== 775, NULL,labels$label_id)
labels$high_profit=ifelse(labels$label_id== 778, 1, 0)#Creates a flag for low profitability, High Profit,high_profit
labels$label_id = ifelse(labels$label_id== 778, NULL,labels$label_id)
labels$industry_tag=ifelse(labels$label_id== 548, 1, 0)#Creates a flag for industry activity more youth
labels$label_id = ifelse(labels$label_id== 548, NULL,labels$label_id)
labels$class_elim=ifelse(labels$label_id== 813, 1, 0)#Creates a flag for Elimination of Class, higher with young females 
labels$label_id = ifelse(labels$label_id== 813, NULL,labels$label_id)
labels$p2p=ifelse(labels$label_id== 757, 1, 0)#Creates a flag for peer-to-peer, higher with older men
labels$label_id = ifelse(labels$label_id== 757, NULL,labels$label_id)
labels$label621 =ifelse(labels$label_id== 621, 1, 0) #Creates a flag for 621
labels$label_id <- ifelse(labels$label_id == 621, NULL, labels$label_id) 
labels$label464=ifelse(labels$label_id== 464, 1, 0) # Creates a flag for label 464
labels$label_id <- ifelse(labels$label_id == 464, NULL, labels$label_id) 
labels$label631 =ifelse(labels$label_id== 631, 1, 0)#Creates a flag for label 631
labels$label_id <- ifelse(labels$label_id == 631, NULL, labels$label_id) 
labels$label318 =ifelse(labels$label_id== 318, 1, 0)#Creates a flag for label 318
labels$label_id <- ifelse(labels$label_id == 318, NULL, labels$label_id) 
labels$label645 =ifelse(labels$label_id== 645, 1, 0)#Creates a flag for label 645
labels$label_id <- ifelse(labels$label_id == 645, NULL, labels$label_id) 

rm(label_cat,app_labels)
names(labels) #"label_id"     "category"     "big_category" "app_id"   
setkey(labels, "app_id")
labels <- data.table(labels)
summary(labels)

## Merge large mobile data table with labels
mobile.train <- merge(labels, mobile2, by = "app_id", allow.cartesian=TRUE)
mobile.train <- data.table(mobile.train)
#Changing gender, group, phone_brand, and device_model into factor types
mobile.train$gender <- as.factor(mobile.train$gender)
mobile.train$group <- as.factor(mobile.train$group)
mobile.train$phone_brand <- as.factor(mobile.train$phone_brand)
mobile.train$device_model <- as.factor(mobile.train$device_model)
mobile.train$category <- as.factor(mobile.train$category)
mobile.train$big_category <- as.factor(mobile.train$big_category)
mobile.train$label_id <- as.factor(mobile.train$label_id)

#Reducing final_merged_data to only device ids in the training set#

n_distinct(mobile.train$device_id) #17,278 as expected

summary(mobile.train)

#Creating each column for the final training set
by_device_id_train_counts_1 <- mobile.train %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    event_count = n_distinct(event_id, na.rm = TRUE), 
    app_id_unique_count = n_distinct(app_id, na.rm = TRUE), 
    is_active_sum = sum(is_active, na.rm = TRUE),
    label_id_unique_count = n_distinct(label_id, na.rm = TRUE),
    timestamp_unique_count = n_distinct(timestamp, na.rm = TRUE),
    earilest_timestamp = min(timestamp),
    latest_timestamp = max(timestamp),
    days_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "days"),
    hours_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "hours"),
    big_category_unique_count = n_distinct(big_category, na.rm = TRUE)
  )

by_device_id_train_counts_2 <- mobile.train %>%
  group_by(
    device_id,
    big_category
  ) %>%
  dplyr::summarize(
    category_count = n()
  ) %>%
  filter(
    !is.na(big_category)
  ) %>%
  spread(
    big_category,
    category_count,
    fill = 0
  )

names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2)] <- str_c(
  'bc_',
  str_replace_all(
    names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2 )],
    ' ',
    ''
  )
)
mobile.train$longitude <- NULL
mobile.train$latitude <- NULL
mobile.train$timestamp <- NULL
mobile.train$big_category <- NULL

#merging counts together and adding demographics back into training set
mobile.train <- left_join(by_device_id_train_counts_1, by_device_id_train_counts_2, by = 'device_id') %>%
  left_join(mobile.train, by = "device_id")

rm(by_device_id_train_counts_1)
rm(by_device_id_train_counts_2)
rm(mobile2, labels)
mobile.train$event_id <- NULL
setkey(mobile.train, NULL)
mobile.train <- unique(mobile.train)
dim(mobile.train) #10236146       73

summary(mobile.train)
#Combining labels that have same breakdown by age/gender group to reduce dimensionality
mobile.train$label_id <- ifelse(mobile.train$label_id == 732, 731, mobile.train$label_id) 
mobile.train$label_id <- ifelse(mobile.train$label_id == 279, 222, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 747, 255, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 749, 255, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 937, 933, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 141, 246, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 152, 246, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 818, 246, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 820, 246, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 738, 737, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 205, 204, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 206, 204, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 843, 169, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 271, 169, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 840, 169, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 558, 551, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 758, 254, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 795,  27, mobile.train$label_id)
mobile.train$take_away=ifelse(mobile.train$label_id== 211, 1, 0)#Creates a flag for 211, Take-Away ordering, which has high number of females 24-26 years old,flag_4
mobile.train$video_tag=ifelse(mobile.train$label_id== 179, 1, 0)#Creates a flag for video, higher with young people
mobile.train$im_tag=ifelse(mobile.train$label_id== 172, 1, 0)#Creates a flag for IM, higher with females 
mobile.train$label_id <- ifelse(mobile.train$label_id == 211, NULL, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 179, NULL, mobile.train$label_id)
mobile.train$label_id <- ifelse(mobile.train$label_id == 172, NULL, mobile.train$label_id)
summary(mobile.train)

#Creating flags for labels that have different breakdown by age/gender group
mobile.train$label463 =ifelse(mobile.train$label_id== 463, 1, 0) # Creates a flag for 463
mobile.train$label_id <- ifelse(mobile.train$label_id == 463, NULL, mobile.train$label_id) 

mobile.train$industry_cat=ifelse(mobile.train$category== 'Industry tag', 1, 0)#Creates a flag for Industry tag category
mobile.train$category <- ifelse(mobile.train$category == 'Industry tag', NULL, mobile.train$label_id)

mobile.train$High_risk=ifelse(mobile.train$category== 'High risk', 1, 0)#Creates a flag for Industry tag category
mobile.train$category <- ifelse(mobile.train$category == 'High risk', NULL, mobile.train$label_id)

mobile.train$low_risk=ifelse((mobile.train$category== 'Low risk'| mobile.train$category== 'Low Risk'), 1, 0)#Creates a flag for Industry tag category
mobile.train$category <- ifelse((mobile.train$category== 'Low risk'| mobile.train$category== 'Low Risk'), NULL, mobile.train$label_id)

mobile.train$Higher_risk=ifelse(mobile.train$category== 'Higher risk', 1, 0)#Creates a flag for Industry tag category
mobile.train$category <- ifelse(mobile.train$category == 'Higher risk', NULL, mobile.train$label_id)

mobile.train$medium_risk=ifelse(mobile.train$category== 'Medium risk', 1, 0)#Creates a flag for Industry tag category
mobile.train$category <- ifelse(mobile.train$category == 'Medium risk', NULL, mobile.train$label_id)

mobile.train$property=ifelse((mobile.train$category== 'Property Industry 2.0'|mobile.train$category== 'Property Industry 1.0'), 1, 0)#Creates a flag for property category
mobile.train$category <- ifelse((mobile.train$category== 'Property Industry 2.0'|mobile.train$category== 'Property Industry 1.0'), NULL, mobile.train$label_id)

mobile.train$services=ifelse(mobile.train$services== 'Services 1', 1, 0)#Creates a flag for services category
mobile.train$category <- ifelse(mobile.train$category == 'Services 1', NULL, mobile.train$label_id)

mobile.train$custom=ifelse(mobile.train$category== 'Custom label', 1, 0)#Creates a flag for custom label category
mobile.train$category <- ifelse(mobile.train$category == 'Custom label', NULL, mobile.train$label_id)

mobile.train$label_id <- NULL


dim(mobile.train) #26482814       76
#Remove old object and check memory allocation
summary(mobile.train) 
head(mobile.train)
names(mobile.train) # [1]  "app_id"       "label_id"     "category"     "big_category" "flag_1"       "event_id"     "is_active"    "device_id"    "timestamp"   
#[10] "longitude"    "latitude"     "gender"       "age"          "group"        "phone_brand"  "device_model"
setkey(mobile.train, NULL)
unique_devices <- unique(mobile.train$device_id)
summary(unique_devices) #Length  7787 devices

## Create flags for specific phone brands
mobile.train$meilu=ifelse(mobile.train$phone_brand == 'Meilu', 1, 0)#Creates a flag for Meilu, higher with men 23-26
mobile.train$vivo=ifelse(mobile.train$phone_brand == 'vivo', 1, 0)#Creates a flag for vivo, higher with women under 23
mobile.train$jinli=ifelse(mobile.train$phone_brand == 'Jinli', 1, 0)#Creates a flag for Jinli, higher with men over 39
mobile.train$CNMO=ifelse(mobile.train$phone_brand == 'CNMO', 1, 0)#Creates a flag for CNMO, higher with men

names(mobile.train)
mobile.train.2 <- mobile.train[,c(58:60,1:43,45:56, 61:77)]
setkey(mobile.train.2, NULL)
rm(mobile.train, brand)
mobile.train.2 <- unique(mobile.train.2)

names(mobile.train.2)
summary(mobile.train.2) #Length  7787 devices
head(mobile.train.2)
#writing the final train dataset to a csv
write_csv(mobile.train.2, "CJ.mobile.train2.csv")
summary(mobile.train.2)
############### Validation Dataset ####################################################
rm(list=ls())
library(lars); library(caret); library(e1071); 
library(class);library(leaps); library(Hmisc); library(plyr); library(devtools); library(ggplot2); # Data visualization
library(readr); # CSV file I/O, e.g. the read_csv function; 
library(lubridate); library(date) # date functions
library(data.table); # Using data.table to speed up runtime
library(repr); # Use repr to adjust plot size
library(profvis); library(dplyr); library(ggmap); library(knitr); library(arules); library(scales) #for scaling dates
library(reshape2); library(bit64); library(showtext);library(zoo); library(dummy);library(tidyr); library(mice); library(stringr)
require(data.table)
library(doParallel); registerDoParallel(cores = 4)
memory.limit(size=60000)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

# load the data
gat.tr <- fread("~/Northwestern University/PREDICT 498/DATA/gender_age_train.csv", head=T, colClasses = c("character", "character", "integer", "character"))# load the "gender, age training data" file
names(gat.tr) #"device_id" "gender"    "age"       "group" 

setkeyv(gat.tr, "device_id")

dim(gat.tr) # 74645     4
summary(gat.tr) #device_id  gender  age  group

set.seed(1306) # set random number generator seed to enable
# repeatability of results
n <- dim(gat.tr)[1]
tr.val.test <- sample(n, round(n/3)) # randomly sample 33% train, validate, and test
ga.train <- gat.tr[tr.val.test,]
ga.train.val <- gat.tr[-tr.val.test,]
vt <- dim(ga.train.val)[1]
val.train <- sample(vt, round(vt/2)) # randomly sample remaining 66% into validate, and test
ga.val <- ga.train.val [-val.train,]
ga.test <- ga.train.val [val.train,]

dim(ga.train) #[1] 24882     4
summary(ga.train)
dim(ga.val) #[1] 24881     4
summary(ga.val)
dim(ga.test) #[1] 24882     4
summary(ga.test)
rm(ga.train.val, ga.train, gat.tr)
#Reloading third file brand Device Type data and setting device ID as the key to create test dataset
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186722      3
setkey(brand,device_id)

#Loading second file brand Device Type data and setting device ID as the key
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3
brand$phone_brand <- as.factor(brand$phone_brand)
brand$device_model <- as.factor(brand$device_model)
brand$phone_brand <- collapsecategory(brand$phone_brand, p = .01)
brand$device_model <- collapsecategory(brand$device_model, p = .01)

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186721      3
setkey(brand,device_id)
summary(brand)
brand.val <- merge(ga.val,brand,by="device_id")


#Remove old object to save memory **Very important with dataset this large**
rm(ga.val)
head(brand.val)
dim(brand.val) # 24883     6
head(brand.val)
summary(brand.val)

#Add event data using device_id as key
events <- read.csv("~/Northwestern University/PREDICT 498/DATA/events.csv", header=T, numerals='no.loss') # load the "mobile events" file 
names(events) # "event_id"  "device_id" "timestamp" "longitude" "latitude" 

dim(events) #3252950       5
#Remove duplicates: None found
#setkey(events,NULL)
#events <-unique(events)
#dim(events) #[1] 3252950       5
events$timestamp <- as.POSIXct((events$timestamp), format=c('%Y-%m-%d %H:%M:%S'))
events$event_id <- as.character(events$event_id)
events$device_id <- as.character(events$device_id)
events <- data.table(events)
events$latitude <- round(events$latitude)
events$longitude <- round(events$longitude)
summary(events)
setkey(events,device_id)

dim(events) #3252950, 5


gat_br_events <- merge(events, brand.val, by="device_id")

#Remove old objects
rm(brand.val,events)
dim(gat_br_events) #399496     10
summary(gat_br_events)
gat_br_events <- data.table(gat_br_events) # device_id event_id timestamp longitude latitude gender age  group phone_brand device_model

#Change key to event ID to merge with app events
setkey(gat_br_events,event_id)


#Add app events and merge to new dataset
app_events <- read.csv("~/Northwestern University/PREDICT 498/DATA/app_events.csv", colClasses = c("character", "character","numeric", "numeric"))
# load the "application events" file and remove "is_installed" since all ==1
app_events<-app_events[,c(1,2,4)]
app_events$event_id <- as.character(app_events$event_id)
names(app_events) #"event_id"     "app_id"   "is_active" 
app_events<- data.table(app_events)
setkey(app_events,event_id) #
dim(app_events) # 32473067        3

mobile2 <- merge(app_events,gat_br_events,by="event_id")

rm(gat_br_events, app_events)
mobile2 <- data.table(mobile2)
summary(mobile2)
dim(mobile2) # 4104969      12
head(mobile2)

#Change key to app_id to merge app_label data
setkey(mobile2,app_id)
head(mobile2)

##Load app label data and merge with label categories before merging with mobile2
app_labels <- fread("~/Northwestern University/PREDICT 498/DATA/app_labels.csv", colClasses = c("character", "character")) # load the "app labels" file
names(app_labels) #"app_id"   "label_id"
app_labels <- data.table(app_labels)
app_labels$label_id <- as.factor(app_labels$label_id)
setkey(app_labels,label_id) #
dim(app_labels) #459943     22
summary(app_labels)

#Load Label Cateogries
label_cat <- read.csv("~/Northwestern University/PREDICT 498/DATA/label_categories_big2.csv", colClasses = c("character", "character")) # load the "label categories" file
names(label_cat) #"label_id" "category" "big_category"
label_cat <- data.table(label_cat)
setkey(label_cat,label_id)
dim(label_cat) #930   3

#Find and remove duplicate entries
dim(label_cat) #
setkey(label_cat, NULL)
labels <- unique(label_cat)
dim(label_cat) #

label_cat$label_id <- as.factor(label_cat$label_id)

summary(label_cat) 
label_cat <- data.table(label_cat)
labels <- merge(label_cat,app_labels,by="label_id", allow.cartesian=TRUE)
labels$flag_1=ifelse(labels$label_id== 773, 1, 0)#Creates a flag for 773,High Flow,flag_1
labels$label_id = ifelse(labels$label_id== 773, NULL,labels$label_id)
labels$flag_3=ifelse(labels$label_id== 188, 1, 0)#Creates a flag for 188, Flight, which has high number of females 24-26 years old,flag_3
labels$label_id = ifelse(labels$label_id== 188, NULL,labels$label_id)
labels$flag_5=ifelse(labels$label_id== 775, 1, 0)#Creates a flag for 775, Liquid Medium, which has high number of older people,flag_5
labels$label_id = ifelse(labels$label_id== 775, NULL,labels$label_id)
labels$high_profit=ifelse(labels$label_id== 778, 1, 0)#Creates a flag for low profitability, High Profit,high_profit
labels$label_id = ifelse(labels$label_id== 778, NULL,labels$label_id)
labels$industry_tag=ifelse(labels$label_id== 548, 1, 0)#Creates a flag for industry activity more youth
labels$label_id = ifelse(labels$label_id== 548, NULL,labels$label_id)
labels$class_elim=ifelse(labels$label_id== 813, 1, 0)#Creates a flag for Elimination of Class, higher with young females 
labels$label_id = ifelse(labels$label_id== 813, NULL,labels$label_id)
labels$p2p=ifelse(labels$label_id== 757, 1, 0)#Creates a flag for peer-to-peer, higher with older men
labels$label_id = ifelse(labels$label_id== 757, NULL,labels$label_id)
labels$label621 =ifelse(labels$label_id== 621, 1, 0) #Creates a flag for 621
labels$label_id <- ifelse(labels$label_id == 621, NULL, labels$label_id) 
labels$label464=ifelse(labels$label_id== 464, 1, 0) # Creates a flag for label 464
labels$label_id <- ifelse(labels$label_id == 464, NULL, labels$label_id) 
labels$label631 =ifelse(labels$label_id== 631, 1, 0)#Creates a flag for label 631
labels$label_id <- ifelse(labels$label_id == 631, NULL, labels$label_id) 
labels$label318 =ifelse(labels$label_id== 318, 1, 0)#Creates a flag for label 318
labels$label_id <- ifelse(labels$label_id == 318, NULL, labels$label_id) 
labels$label645 =ifelse(labels$label_id== 645, 1, 0)#Creates a flag for label 645
labels$label_id <- ifelse(labels$label_id == 645, NULL, labels$label_id) 

rm(label_cat,app_labels)
names(labels) #"label_id"     "category"     "big_category" "app_id"   
setkey(labels, "app_id")
labels <- data.table(labels)
summary(labels)

## Merge large mobile data table with labels
mobile.val <- merge(labels, mobile2, by = "app_id", allow.cartesian=TRUE)
mobile.val <- data.table(mobile.val)
#Changing gender, group, phone_brand, and device_model into factor types
mobile.val$gender <- as.factor(mobile.val$gender)
mobile.val$group <- as.factor(mobile.val$group)
mobile.val$phone_brand <- as.factor(mobile.val$phone_brand)
mobile.val$device_model <- as.factor(mobile.val$device_model)
mobile.val$category <- as.factor(mobile.val$category)
mobile.val$big_category <- as.factor(mobile.val$big_category)
mobile.val$label_id <- as.factor(mobile.val$label_id)

#Reducing final_merged_data to only device ids in the training set#

n_distinct(mobile.val$device_id) #17,278 as expected

summary(mobile.val)

#Creating each column for the final training set
by_device_id_train_counts_1 <- mobile.val %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    event_count = n_distinct(event_id, na.rm = TRUE), 
    app_id_unique_count = n_distinct(app_id, na.rm = TRUE), 
    is_active_sum = sum(is_active, na.rm = TRUE),
    label_id_unique_count = n_distinct(label_id, na.rm = TRUE),
    timestamp_unique_count = n_distinct(timestamp, na.rm = TRUE),
    earilest_timestamp = min(timestamp),
    latest_timestamp = max(timestamp),
    days_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "days"),
    hours_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "hours"),
    big_category_unique_count = n_distinct(big_category, na.rm = TRUE)
  )

by_device_id_train_counts_2 <- mobile.val %>%
  group_by(
    device_id,
    big_category
  ) %>%
  dplyr::summarize(
    category_count = n()
  ) %>%
  filter(
    !is.na(big_category)
  ) %>%
  spread(
    big_category,
    category_count,
    fill = 0
  )

names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2)] <- str_c(
  'bc_',
  str_replace_all(
    names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2 )],
    ' ',
    ''
  )
)
mobile.val$longitude <- NULL
mobile.val$latitude <- NULL
mobile.val$timestamp <- NULL
mobile.val$big_category <- NULL

#merging counts together and adding demographics back into training set
mobile.val <- left_join(by_device_id_train_counts_1, by_device_id_train_counts_2, by = 'device_id') %>%
  left_join(mobile.val, by = "device_id")

rm(by_device_id_train_counts_1)
rm(by_device_id_train_counts_2)
rm(mobile2, labels)
mobile.val$event_id <- NULL
setkey(mobile.val, NULL)
mobile.val <- unique(mobile.val)
dim(mobile.val) #10236146       73

summary(mobile.val)
#Combining labels that have same breakdown by age/gender group to reduce dimensionality
mobile.val$label_id <- ifelse(mobile.val$label_id == 732, 731, mobile.val$label_id) 
mobile.val$label_id <- ifelse(mobile.val$label_id == 279, 222, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 747, 255, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 749, 255, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 937, 933, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 141, 246, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 152, 246, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 818, 246, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 820, 246, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 738, 737, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 205, 204, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 206, 204, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 843, 169, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 271, 169, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 840, 169, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 558, 551, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 758, 254, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 795,  27, mobile.val$label_id)
mobile.val$take_away=ifelse(mobile.val$label_id== 211, 1, 0)#Creates a flag for 211, Take-Away ordering, which has high number of females 24-26 years old,flag_4
mobile.val$video_tag=ifelse(mobile.val$label_id== 179, 1, 0)#Creates a flag for video, higher with young people
mobile.val$im_tag=ifelse(mobile.val$label_id== 172, 1, 0)#Creates a flag for IM, higher with females 
mobile.val$label_id <- ifelse(mobile.val$label_id == 211, NULL, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 179, NULL, mobile.val$label_id)
mobile.val$label_id <- ifelse(mobile.val$label_id == 172, NULL, mobile.val$label_id)
summary(mobile.val)

#Creating flags for labels that have different breakdown by age/gender group
mobile.val$label463 =ifelse(mobile.val$label_id== 463, 1, 0) # Creates a flag for 463
mobile.val$label_id <- ifelse(mobile.val$label_id == 463, NULL, mobile.val$label_id) 

mobile.val$industry_cat=ifelse(mobile.val$category== 'Industry tag', 1, 0)#Creates a flag for Industry tag category
mobile.val$category <- ifelse(mobile.val$category == 'Industry tag', NULL, mobile.val$label_id)

mobile.val$High_risk=ifelse(mobile.val$category== 'High risk', 1, 0)#Creates a flag for Industry tag category
mobile.val$category <- ifelse(mobile.val$category == 'High risk', NULL, mobile.val$label_id)

mobile.val$low_risk=ifelse((mobile.val$category== 'Low risk'| mobile.val$category== 'Low Risk'), 1, 0)#Creates a flag for Industry tag category
mobile.val$category <- ifelse((mobile.val$category== 'Low risk'| mobile.val$category== 'Low Risk'), NULL, mobile.val$label_id)

mobile.val$Higher_risk=ifelse(mobile.val$category== 'Higher risk', 1, 0)#Creates a flag for Industry tag category
mobile.val$category <- ifelse(mobile.val$category == 'Higher risk', NULL, mobile.val$label_id)

mobile.val$medium_risk=ifelse(mobile.val$category== 'Medium risk', 1, 0)#Creates a flag for Industry tag category
mobile.val$category <- ifelse(mobile.val$category == 'Medium risk', NULL, mobile.val$label_id)

mobile.val$property=ifelse((mobile.val$category== 'Property Industry 2.0'|mobile.val$category== 'Property Industry 1.0'), 1, 0)#Creates a flag for property category
mobile.val$category <- ifelse((mobile.val$category== 'Property Industry 2.0'|mobile.val$category== 'Property Industry 1.0'), NULL, mobile.val$label_id)

mobile.val$services=ifelse(mobile.val$services== 'Services 1', 1, 0)#Creates a flag for services category
mobile.val$category <- ifelse(mobile.val$category == 'Services 1', NULL, mobile.val$label_id)

mobile.val$custom=ifelse(mobile.val$category== 'Custom label', 1, 0)#Creates a flag for custom label category
mobile.val$category <- ifelse(mobile.val$category == 'Custom label', NULL, mobile.val$label_id)

mobile.val$label_id <- NULL


dim(mobile.val) #26482814       76
#Remove old object and check memory allocation
summary(mobile.val) 
head(mobile.val)
names(mobile.val) # [1]  "app_id"       "label_id"     "category"     "big_category" "flag_1"       "event_id"     "is_active"    "device_id"    "timestamp"   
#[10] "longitude"    "latitude"     "gender"       "age"          "group"        "phone_brand"  "device_model"
setkey(mobile.val, NULL)
unique_devices <- unique(mobile.val$device_id)
summary(unique_devices) #Length  7787 devices

## Create flags for specific phone brands
mobile.val$meilu=ifelse(mobile.val$phone_brand == 'Meilu', 1, 0)#Creates a flag for Meilu, higher with men 23-26
mobile.val$vivo=ifelse(mobile.val$phone_brand == 'vivo', 1, 0)#Creates a flag for vivo, higher with women under 23
mobile.val$jinli=ifelse(mobile.val$phone_brand == 'Jinli', 1, 0)#Creates a flag for Jinli, higher with men over 39
mobile.val$CNMO=ifelse(mobile.val$phone_brand == 'CNMO', 1, 0)#Creates a flag for CNMO, higher with men



#library(DataExplorer);
#GenerateReport(mobile.val, output_file = "cj_mobile.val_eda_report.html")
#men <- mobile.val[ which(mobile.val$gender=='M'),]
#women <- mobile.val[ which(mobile.val$gender=='F'),]
#head(men)
#summary(men)
#dim(men)
#head(women)
#summary(women)
#dim(women)
#GenerateReport(data.val, output_file = "td_eda_report.html")
#GenerateReport(men, output_file = "men_td_eda_report.html")
#GenerateReport(women, output_file = "women_td_eda_report.html")
names(mobile.val)
mobile.val.2 <- mobile.val[,c(58:60,1:42,45:56, 61:77)]
summary(mobile.val.2)
setkey(mobile.val.2, NULL)
#rm(mobile.val, brand)
mobile.val.2 <- unique(mobile.val.2)

names(mobile.val.2)
dim(mobile.val.2) #1035648      73
head(mobile.val.2)
#writing the final train dataset to a csv
write_csv(mobile.val.2, "CJ.mobile.val2.csv")
summary(mobile.val.2)

#################  Add Test Data to create .csv #########################
rm(list=ls())
library(lars); library(caret); library(e1071); 
library(class);library(leaps); library(Hmisc); library(plyr); library(devtools); library(ggplot2); # Data visualization
library(readr); # CSV file I/O, e.g. the read_csv function; 
library(lubridate); library(date) # date functions
library(data.table); # Using data.table to speed up runtime
library(repr); # Use repr to adjust plot size
library(profvis); library(dplyr); library(ggmap); library(knitr); library(arules); library(scales) #for scaling dates
library(reshape2); library(bit64); library(showtext);library(zoo); library(dummy);library(tidyr); library(mice); library(stringr)
require(data.table)
library(doParallel); registerDoParallel(cores = 4)
memory.limit(size=60000)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

# load the data
gat.tr <- fread("~/Northwestern University/PREDICT 498/DATA/gender_age_train.csv", head=T, colClasses = c("character", "character", "integer", "character"))# load the "gender, age training data" file
names(gat.tr) #"device_id" "gender"    "age"       "group" 

setkeyv(gat.tr, "device_id")

dim(gat.tr) # 74645     4
summary(gat.tr) #device_id  gender  age  group

set.seed(1306) # set random number generator seed to enable
# repeatability of results
n <- dim(gat.tr)[1]
tr.val.test <- sample(n, round(n/3)) # randomly sample 33% train, validate, and test
ga.train <- gat.tr[tr.val.test,]
ga.train.val <- gat.tr[-tr.val.test,]
vt <- dim(ga.train.val)[1]
val.train <- sample(vt, round(vt/2)) # randomly sample remaining 66% into validate, and test
ga.val <- ga.train.val [-val.train,]
ga.test <- ga.train.val [val.train,]

dim(ga.train) #[1] 24882     4
summary(ga.train)
dim(ga.val) #[1] 24881     4
summary(ga.val)
dim(ga.test) #[1] 24882     4
summary(ga.test)
rm(ga.train.val, ga.train, gat.tr)
#Reloading third file brand Device Type data and setting device ID as the key to create test dataset
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186722      3
setkey(brand,device_id)

#Loading second file brand Device Type data and setting device ID as the key
brand <- fread("~/Northwestern University/PREDICT 498/DATA/Brand_Find_Replace2.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3
brand$phone_brand <- as.factor(brand$phone_brand)
brand$device_model <- as.factor(brand$device_model)
brand$phone_brand <- collapsecategory(brand$phone_brand, p = .01)
brand$device_model <- collapsecategory(brand$device_model, p = .01)

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand) #186721      3
setkey(brand,device_id)
summary(brand)
brand.test <- merge(ga.test,brand,by="device_id")


#Remove old object to save memory **Very important with dataset this large**
rm(ga.test)
head(brand.test)
dim(brand.test) # 24883     6
head(brand.test)
summary(brand.test)

#Add event data using device_id as key
events <- read.csv("~/Northwestern University/PREDICT 498/DATA/events.csv", header=T, numerals='no.loss') # load the "mobile events" file 
names(events) # "event_id"  "device_id" "timestamp" "longitude" "latitude" 

dim(events) #3252950       5
#Remove duplicates: None found
#setkey(events,NULL)
#events <-unique(events)
#dim(events) #[1] 3252950       5
events$timestamp <- as.POSIXct((events$timestamp), format=c('%Y-%m-%d %H:%M:%S'))
events$event_id <- as.character(events$event_id)
events$device_id <- as.character(events$device_id)
events <- data.table(events)
events$latitude <- round(events$latitude)
events$longitude <- round(events$longitude)
summary(events)
setkey(events,device_id)

dim(events) #3252950, 5


gat_br_events <- merge(events, brand.test, by="device_id")

#Remove old objects
rm(brand.test,events)
dim(gat_br_events) #399496     10
summary(gat_br_events)
gat_br_events <- data.table(gat_br_events) # device_id event_id timestamp longitude latitude gender age  group phone_brand device_model

#Change key to event ID to merge with app events
setkey(gat_br_events,event_id)


#Add app events and merge to new dataset
app_events <- read.csv("~/Northwestern University/PREDICT 498/DATA/app_events.csv", colClasses = c("character", "character","numeric", "numeric"))
# load the "application events" file and remove "is_installed" since all ==1
app_events<-app_events[,c(1,2,4)]
app_events$event_id <- as.character(app_events$event_id)
names(app_events) #"event_id"     "app_id"   "is_active" 
app_events<- data.table(app_events)
setkey(app_events,event_id) #
dim(app_events) # 32473067        3

mobile2 <- merge(app_events,gat_br_events,by="event_id")

rm(gat_br_events, app_events)
mobile2 <- data.table(mobile2)
summary(mobile2)
dim(mobile2) # 4104969      12
head(mobile2)

#Change key to app_id to merge app_label data
setkey(mobile2,app_id)
head(mobile2)

##Load app label data and merge with label categories before merging with mobile2
app_labels <- fread("~/Northwestern University/PREDICT 498/DATA/app_labels.csv", colClasses = c("character", "character")) # load the "app labels" file
names(app_labels) #"app_id"   "label_id"
app_labels <- data.table(app_labels)
app_labels$label_id <- as.factor(app_labels$label_id)
setkey(app_labels,label_id) #
dim(app_labels) #459943     22
summary(app_labels)

#Load Label Cateogries
label_cat <- read.csv("~/Northwestern University/PREDICT 498/DATA/label_categories_big2.csv", colClasses = c("character", "character")) # load the "label categories" file
names(label_cat) #"label_id" "category" "big_category"
label_cat <- data.table(label_cat)
setkey(label_cat,label_id)
dim(label_cat) #930   3

#Find and remove duplicate entries
dim(label_cat) #
setkey(label_cat, NULL)
labels <- unique(label_cat)
dim(label_cat) #

label_cat$label_id <- as.factor(label_cat$label_id)

summary(label_cat) 
label_cat <- data.table(label_cat)
labels <- merge(label_cat,app_labels,by="label_id", allow.cartesian=TRUE)
labels$flag_1=ifelse(labels$label_id== 773, 1, 0)#Creates a flag for 773,High Flow,flag_1
labels$label_id = ifelse(labels$label_id== 773, NULL,labels$label_id)
labels$flag_3=ifelse(labels$label_id== 188, 1, 0)#Creates a flag for 188, Flight, which has high number of females 24-26 years old,flag_3
labels$label_id = ifelse(labels$label_id== 188, NULL,labels$label_id)
labels$flag_5=ifelse(labels$label_id== 775, 1, 0)#Creates a flag for 775, Liquid Medium, which has high number of older people,flag_5
labels$label_id = ifelse(labels$label_id== 775, NULL,labels$label_id)
labels$high_profit=ifelse(labels$label_id== 778, 1, 0)#Creates a flag for low profitability, High Profit,high_profit
labels$label_id = ifelse(labels$label_id== 778, NULL,labels$label_id)
labels$industry_tag=ifelse(labels$label_id== 548, 1, 0)#Creates a flag for industry activity more youth
labels$label_id = ifelse(labels$label_id== 548, NULL,labels$label_id)
labels$class_elim=ifelse(labels$label_id== 813, 1, 0)#Creates a flag for Elimination of Class, higher with young females 
labels$label_id = ifelse(labels$label_id== 813, NULL,labels$label_id)
labels$p2p=ifelse(labels$label_id== 757, 1, 0)#Creates a flag for peer-to-peer, higher with older men
labels$label_id = ifelse(labels$label_id== 757, NULL,labels$label_id)
labels$label621 =ifelse(labels$label_id== 621, 1, 0) #Creates a flag for 621
labels$label_id <- ifelse(labels$label_id == 621, NULL, labels$label_id) 
labels$label464=ifelse(labels$label_id== 464, 1, 0) # Creates a flag for label 464
labels$label_id <- ifelse(labels$label_id == 464, NULL, labels$label_id) 
labels$label631 =ifelse(labels$label_id== 631, 1, 0)#Creates a flag for label 631
labels$label_id <- ifelse(labels$label_id == 631, NULL, labels$label_id) 
labels$label318 =ifelse(labels$label_id== 318, 1, 0)#Creates a flag for label 318
labels$label_id <- ifelse(labels$label_id == 318, NULL, labels$label_id) 
labels$label645 =ifelse(labels$label_id== 645, 1, 0)#Creates a flag for label 645
labels$label_id <- ifelse(labels$label_id == 645, NULL, labels$label_id) 

rm(label_cat,app_labels)
names(labels) #"label_id"     "category"     "big_category" "app_id"   
setkey(labels, "app_id")
labels <- data.table(labels)
summary(labels)

## Merge large mobile data table with labels
mobile.test <- merge(labels, mobile2, by = "app_id", allow.cartesian=TRUE)
mobile.test <- data.table(mobile.test)
#Changing gender, group, phone_brand, and device_model into factor types
mobile.test$gender <- as.factor(mobile.test$gender)
mobile.test$group <- as.factor(mobile.test$group)
mobile.test$phone_brand <- as.factor(mobile.test$phone_brand)
mobile.test$device_model <- as.factor(mobile.test$device_model)
mobile.test$category <- as.factor(mobile.test$category)
mobile.test$big_category <- as.factor(mobile.test$big_category)
mobile.test$label_id <- as.factor(mobile.test$label_id)

#Reducing final_merged_data to only device ids in the training set#

n_distinct(mobile.test$device_id) #

summary(mobile.test)

#Creating each column for the final training set
by_device_id_train_counts_1 <- mobile.test %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    event_count = n_distinct(event_id, na.rm = TRUE), 
    app_id_unique_count = n_distinct(app_id, na.rm = TRUE), 
    is_active_sum = sum(is_active, na.rm = TRUE),
    label_id_unique_count = n_distinct(label_id, na.rm = TRUE),
    timestamp_unique_count = n_distinct(timestamp, na.rm = TRUE),
    earilest_timestamp = min(timestamp),
    latest_timestamp = max(timestamp),
    days_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "days"),
    hours_between_first_and_last_timestamp = difftime(max(timestamp, na.rm = TRUE), min(timestamp, na.rm = TRUE), units = "hours"),
    big_category_unique_count = n_distinct(big_category, na.rm = TRUE)
  )

by_device_id_train_counts_2 <- mobile.test %>%
  group_by(
    device_id,
    big_category
  ) %>%
  dplyr::summarize(
    category_count = n()
  ) %>%
  filter(
    !is.na(big_category)
  ) %>%
  spread(
    big_category,
    category_count,
    fill = 0
  )

names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2)] <- str_c(
  'bc_',
  str_replace_all(
    names(by_device_id_train_counts_2)[2:ncol(by_device_id_train_counts_2 )],
    ' ',
    ''
  )
)
mobile.test$longitude <- NULL
mobile.test$latitude <- NULL
mobile.test$timestamp <- NULL
mobile.test$big_category <- NULL

#merging counts together and adding demographics back into training set
mobile.test <- left_join(by_device_id_train_counts_1, by_device_id_train_counts_2, by = 'device_id') %>%
  left_join(mobile.test, by = "device_id")

rm(by_device_id_train_counts_1)
rm(by_device_id_train_counts_2)
rm(mobile2, labels)
mobile.test$event_id <- NULL
setkey(mobile.test, NULL)
mobile.test <- unique(mobile.test)
dim(mobile.test) #10236146       73

summary(mobile.test)
#Combining labels that have same breakdown by age/gender group to reduce dimensionality
mobile.test$label_id <- ifelse(mobile.test$label_id == 732, 731, mobile.test$label_id) 
mobile.test$label_id <- ifelse(mobile.test$label_id == 279, 222, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 747, 255, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 749, 255, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 937, 933, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 141, 246, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 152, 246, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 818, 246, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 820, 246, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 738, 737, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 205, 204, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 206, 204, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 843, 169, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 271, 169, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 840, 169, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 558, 551, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 758, 254, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 795,  27, mobile.test$label_id)
mobile.test$take_away=ifelse(mobile.test$label_id== 211, 1, 0)#Creates a flag for 211, Take-Away ordering, which has high number of females 24-26 years old,flag_4
mobile.test$video_tag=ifelse(mobile.test$label_id== 179, 1, 0)#Creates a flag for video, higher with young people
mobile.test$im_tag=ifelse(mobile.test$label_id== 172, 1, 0)#Creates a flag for IM, higher with females 
mobile.test$label_id <- ifelse(mobile.test$label_id == 211, NULL, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 179, NULL, mobile.test$label_id)
mobile.test$label_id <- ifelse(mobile.test$label_id == 172, NULL, mobile.test$label_id)
summary(mobile.test)

#Creating flags for labels that have different breakdown by age/gender group
mobile.test$label463 =ifelse(mobile.test$label_id== 463, 1, 0) # Creates a flag for 463
mobile.test$label_id <- ifelse(mobile.test$label_id == 463, NULL, mobile.test$label_id) 

mobile.test$industry_cat=ifelse(mobile.test$category== 'Industry tag', 1, 0)#Creates a flag for Industry tag category
mobile.test$category <- ifelse(mobile.test$category == 'Industry tag', NULL, mobile.test$label_id)

mobile.test$High_risk=ifelse(mobile.test$category== 'High risk', 1, 0)#Creates a flag for Industry tag category
mobile.test$category <- ifelse(mobile.test$category == 'High risk', NULL, mobile.test$label_id)

mobile.test$low_risk=ifelse((mobile.test$category== 'Low risk'| mobile.test$category== 'Low Risk'), 1, 0)#Creates a flag for Industry tag category
mobile.test$category <- ifelse((mobile.test$category== 'Low risk'| mobile.test$category== 'Low Risk'), NULL, mobile.test$label_id)

mobile.test$Higher_risk=ifelse(mobile.test$category== 'Higher risk', 1, 0)#Creates a flag for Industry tag category
mobile.test$category <- ifelse(mobile.test$category == 'Higher risk', NULL, mobile.test$label_id)

mobile.test$medium_risk=ifelse(mobile.test$category== 'Medium risk', 1, 0)#Creates a flag for Industry tag category
mobile.test$category <- ifelse(mobile.test$category == 'Medium risk', NULL, mobile.test$label_id)

mobile.test$property=ifelse((mobile.test$category== 'Property Industry 2.0'|mobile.test$category== 'Property Industry 1.0'), 1, 0)#Creates a flag for property category
mobile.test$category <- ifelse((mobile.test$category== 'Property Industry 2.0'|mobile.test$category== 'Property Industry 1.0'), NULL, mobile.test$label_id)

mobile.test$services=ifelse(mobile.test$services== 'Services 1', 1, 0)#Creates a flag for services category
mobile.test$category <- ifelse(mobile.test$category == 'Services 1', NULL, mobile.test$label_id)

mobile.test$custom=ifelse(mobile.test$category== 'Custom label', 1, 0)#Creates a flag for custom label category
mobile.test$category <- ifelse(mobile.test$category == 'Custom label', NULL, mobile.test$label_id)

mobile.test$label_id <- NULL


dim(mobile.test) #26482814       76
#Remove old object and check memory allocation
summary(mobile.test) 
head(mobile.test)
names(mobile.test) # [1]  "app_id"       "label_id"     "category"     "big_category" "flag_1"       "event_id"     "is_active"    "device_id"    "timestamp"   
#[10] "longitude"    "latitude"     "gender"       "age"          "group"        "phone_brand"  "device_model"
setkey(mobile.test, NULL)
unique_devices <- unique(mobile.test$device_id)
summary(unique_devices) #Length  7787 devices

## Create flags for specific phone brands
mobile.test$meilu=ifelse(mobile.test$phone_brand == 'Meilu', 1, 0)#Creates a flag for Meilu, higher with men 23-26
mobile.test$vivo=ifelse(mobile.test$phone_brand == 'vivo', 1, 0)#Creates a flag for vivo, higher with women under 23
mobile.test$jinli=ifelse(mobile.test$phone_brand == 'Jinli', 1, 0)#Creates a flag for Jinli, higher with men over 39
mobile.test$CNMO=ifelse(mobile.test$phone_brand == 'CNMO', 1, 0)#Creates a flag for CNMO, higher with men


names(mobile.test)
mobile.test.2 <- mobile.test[,c(58:60,1:43,45:56, 61:77)]
summary(mobile.test.2)
setkey(mobile.test.2, NULL)
#rm(mobile.test, brand)
mobile.test.2 <- unique(mobile.test.2)

names(mobile.test.2)
dim(mobile.test.2) #1035648      73
head(mobile.test.2)
#writing the final train dataset to a csv
write_csv(mobile.test.2, "CJ.mobile.test2.csv")
summary(mobile.test.2)

mobile_dem <- fread("~/PRED498/DATA/CJ.mobile.train2.csv", head=T) # load the aggregated data file
head(mobile_dem)
summary(mobile_dem)

mobile_dem$hours_btw <- mobile_dem$hours_between_first_and_last_timestamp
mobile_dem$days_btw <- mobile_dem$days_between_first_and_last_timestamp
mobile_dem$device_id <- as.character(mobile_dem$device_id)
mobile_dem$moderate_profit <- NULL
mobile_dem$low_profit <- NULL
mobile_dem$fixed_income <- NULL
mobile_dem$low_income <- NULL
mobile_dem$higher_income <- NULL
mobile_dem$high_risk <- NULL
mobile_dem$medium_risk <- NULL
mobile_dem$low_risk <- NULL
mobile_dem$lowest_risk <- NULL
mobile_dem$high_mob <- NULL
mobile_dem$phone_brand <- as.factor(mobile_dem$phone_brand)
mobile_dem$device_model <- as.factor(mobile_dem$device_model)
mobile_dem$group <- as.factor(mobile_dem$group)
summary(mobile_dem$group)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

mobile_dem$phone_brand <- collapsecategory(mobile_dem$phone_brand, p = .01)
mobile_dem$device_model <- collapsecategory(mobile_dem$device_model, p = .01)
summary(mobile_dem)

mobile_dem2 <- mobile_dem
rm(mobile_dem)
mobile_dem2$app_id <- NULL
mobile_dem2$is_active <- NULL
mobile_dem2$hours_btw <- mobile_dem2$hours_between_first_and_last_timestamp
mobile_dem2$hours_between_first_and_last_timestamp <- NULL
mobile_dem2$days_btw <- mobile_dem2$days_between_first_and_last_timestamp
mobile_dem2$days_between_first_and_last_timestamp <- NULL
mobile_dem2$first_seen <- mobile_dem2$earilest_timestamp
mobile_dem2$earilest_timestamp <- NULL
mobile_dem2$big_cat_count <- mobile_dem2$big_category_unique_count
mobile_dem2$big_category_unique_count <- NULL

by_device_label_count <- mobile_dem2 %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    flag_1 = sum(flag_1, na.rm = TRUE), 
    flag_3 = sum(flag_3, na.rm = TRUE), 
    flag_5 = sum(flag_5, na.rm = TRUE),
    high_profit = sum(high_profit, na.rm = TRUE),
    industry_tag = sum(industry_tag, na.rm = TRUE),
    class_elim = sum(class_elim, na.rm = TRUE),
    p2p = sum(p2p),
    label464 = sum(label464),
    label631 = sum(label631),
    label318 = sum(label318),
    label645 = sum(label645),
    High_risk = sum(High_risk),
    take_away = sum(take_away),
    video_tag = sum(video_tag),
    im_tag = sum(im_tag),
    label463 = sum(label463),
    industry_cat = sum(industry_cat),
    property= sum(property),
    custom = sum(custom),
    label621 = sum(label621)
  )
mobile_dem2$High_risk <- NULL
mobile_dem2$flag_1 <- NULL                 
mobile_dem2$flag_3      <- NULL                     
mobile_dem2$flag_5  <- NULL                        
mobile_dem2$high_profit<- NULL          
mobile_dem2$industry_tag <- NULL             
mobile_dem2$class_elim <- NULL           
mobile_dem2$p2p <- NULL                        
mobile_dem2$label621 <- NULL          
mobile_dem2$label464 <- NULL                
mobile_dem2$label631 <- NULL                 
mobile_dem2$label318 <- NULL                  
mobile_dem2$label645 <- NULL                      
mobile_dem2$take_away <- NULL                  
mobile_dem2$video_tag <- NULL          
mobile_dem2$im_tag <- NULL                        
mobile_dem2$label463 <- NULL                     
mobile_dem2$industry_cat <- NULL          
mobile_dem2$property <- NULL          
mobile_dem2$custom <- NULL           
mobile_dem2$gender <- ifelse(mobile_dem2$gender == 'M', 1, 0)
#merging counts together and adding demographics back into training set
mobile_dem2 <- merge(mobile_dem2, by_device_label_count, by = 'device_id')
names(mobile_dem2)

rm(by_device_label_count)

setkey(mobile_dem2, NULL)
mobile_dem2 <- unique(mobile_dem2)
summary(mobile_dem2)
names(mobile_dem2)
setkey(mobile_dem2, device_id)

################# ADD VALIDATION DATA #############################################
mobile_val <- fread("~/PRED498/DATA/CJ.mobile.val2.csv", head=T) # load the aggregated data file
head(mobile_val)
summary(mobile_val)

mobile_val$hours_btw <- mobile_val$hours_between_first_and_last_timestamp
mobile_val$days_btw <- mobile_val$days_between_first_and_last_timestamp
mobile_val$device_id <- as.character(mobile_val$device_id)
mobile_val$moderate_profit <- NULL
mobile_val$low_profit <- NULL
mobile_val$fixed_income <- NULL
mobile_val$low_income <- NULL
mobile_val$higher_income <- NULL
mobile_val$high_risk <- NULL
mobile_val$medium_risk <- NULL
mobile_val$low_risk <- NULL
mobile_val$lowest_risk <- NULL
mobile_val$high_mob <- NULL
mobile_val$phone_brand <- as.factor(mobile_val$phone_brand)
mobile_val$device_model <- as.factor(mobile_val$device_model)
mobile_val$group <- as.factor(mobile_val$group)
summary(mobile_val$group)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

mobile_val$phone_brand <- collapsecategory(mobile_val$phone_brand, p = .01)
mobile_val$device_model <- collapsecategory(mobile_val$device_model, p = .01)
summary(mobile_val)

mobile_val2 <- mobile_val
rm(mobile_val)
mobile_val2$hours_btw <- mobile_val2$hours_between_first_and_last_timestamp
mobile_val2$hours_between_first_and_last_timestamp <- NULL
mobile_val2$days_btw <- mobile_val2$days_between_first_and_last_timestamp
mobile_val2$days_between_first_and_last_timestamp <- NULL
mobile_val2$first_seen <- mobile_val2$earilest_timestamp
mobile_val2$earilest_timestamp <- NULL
mobile_val2$big_cat_count <- mobile_val2$big_category_unique_count
mobile_val2$big_category_unique_count <- NULL

by_device_label_count <- mobile_val2 %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    flag_1 = sum(flag_1, na.rm = TRUE), 
    flag_3 = sum(flag_3, na.rm = TRUE), 
    flag_5 = sum(flag_5, na.rm = TRUE),
    high_profit = sum(high_profit, na.rm = TRUE),
    industry_tag = sum(industry_tag, na.rm = TRUE),
    class_elim = sum(class_elim, na.rm = TRUE),
    p2p = sum(p2p),
    label464 = sum(label464),
    High_risk = sum(High_risk),
    label631 = sum(label631),
    label318 = sum(label318),
    label645 = sum(label645),
    take_away = sum(take_away),
    video_tag = sum(video_tag),
    im_tag = sum(im_tag),
    label463 = sum(label463),
    industry_cat = sum(industry_cat),
    property= sum(property),
    custom = sum(custom),
    label621 = sum(label621)
  )

mobile_val2$flag_1 <- NULL                 
mobile_val2$flag_3      <- NULL                     
mobile_val2$flag_5  <- NULL                        
mobile_val2$high_profit<- NULL          
mobile_val2$industry_tag <- NULL
mobile_val2$High_risk <- NULL
mobile_val2$class_elim <- NULL           
mobile_val2$p2p <- NULL                        
mobile_val2$label621 <- NULL          
mobile_val2$label464 <- NULL                
mobile_val2$label631 <- NULL                 
mobile_val2$label318 <- NULL                  
mobile_val2$label645 <- NULL                      
mobile_val2$take_away <- NULL                  
mobile_val2$video_tag <- NULL          
mobile_val2$im_tag <- NULL                        
mobile_val2$label463 <- NULL                     
mobile_val2$industry_cat <- NULL          
mobile_val2$property <- NULL          
mobile_val2$custom <- NULL           
mobile_val2$gender <- ifelse(mobile_val2$gender == 'M', 1, 0)
#merging counts together and adding valographics back into training set
mobile_val2 <- merge(mobile_val2, by_device_label_count, by = 'device_id')
names(mobile_val2)

rm(by_device_label_count)

setkey(mobile_val2, NULL)
mobile_val2 <- unique(mobile_val2)
summary(mobile_val2)
names(mobile_val2)
setkey(mobile_val2, device_id)

mobile_tr_val <-rbind(mobile_val2, mobile_dem2)

############# ADD TEST DATASET ################################

mobile_test <- fread("~/PRED498/DATA/CJ.mobile.test2.csv", head=T) # load the aggregated data file
head(mobile_test)
summary(mobile_test)

mobile_test$hours_btw <- mobile_test$hours_between_first_and_last_timestamp
mobile_test$days_btw <- mobile_test$days_between_first_and_last_timestamp
mobile_test$device_id <- as.character(mobile_test$device_id)
mobile_test$moderate_profit <- NULL
mobile_test$low_profit <- NULL
mobile_test$fixed_income <- NULL
mobile_test$low_income <- NULL
mobile_test$higher_income <- NULL
mobile_test$high_risk <- NULL
mobile_test$medium_risk <- NULL
mobile_test$low_risk <- NULL
mobile_test$lowest_risk <- NULL
mobile_test$high_mob <- NULL
mobile_test$phone_brand <- as.factor(mobile_test$phone_brand)
mobile_test$device_model <- as.factor(mobile_test$device_model)
mobile_test$group <- as.factor(mobile_test$group)
summary(mobile_test$group)
collapsecategory <- function(x,p){
  levels_len = length(levels(x))
  levels(x)[levels_len+1] = 'Other'
  y= table(x)/length(x)
  y1 = as.vector(y)
  y2 = names(y)
  y2_len = length(y2)
  for(i in 1:y2_len){
    if(y1[i]<=p){
      x[x == y2[i]] = 'Other'
    }
  }
  x <- droplevels(x)
  x
}

mobile_test$phone_brand <- collapsecategory(mobile_test$phone_brand, p = .01)
mobile_test$device_model <- collapsecategory(mobile_test$device_model, p = .01)
summary(mobile_test)

mobile_test2 <- mobile_test
rm(mobile_test)
mobile_test2$app_id <- NULL
mobile_test2$is_active <- NULL
mobile_test2$hours_btw <- mobile_test2$hours_between_first_and_last_timestamp
mobile_test2$hours_between_first_and_last_timestamp <- NULL
mobile_test2$days_btw <- mobile_test2$days_between_first_and_last_timestamp
mobile_test2$days_between_first_and_last_timestamp <- NULL
mobile_test2$first_seen <- mobile_test2$earilest_timestamp
mobile_test2$earilest_timestamp <- NULL
mobile_test2$big_cat_count <- mobile_test2$big_category_unique_count
mobile_test2$big_category_unique_count <- NULL

by_device_label_count <- mobile_test2 %>%
  group_by(
    device_id
  ) %>%
  dplyr::summarize(
    flag_1 = sum(flag_1, na.rm = TRUE), 
    flag_3 = sum(flag_3, na.rm = TRUE), 
    flag_5 = sum(flag_5, na.rm = TRUE),
    high_profit = sum(high_profit, na.rm = TRUE),
    industry_tag = sum(industry_tag, na.rm = TRUE),
    class_elim = sum(class_elim, na.rm = TRUE),
    p2p = sum(p2p),
    label464 = sum(label464),
    label631 = sum(label631),
    label318 = sum(label318),
    label645 = sum(label645),
    High_risk = sum(High_risk),
    take_away = sum(take_away),
    video_tag = sum(video_tag),
    im_tag = sum(im_tag),
    label463 = sum(label463),
    industry_cat = sum(industry_cat),
    property= sum(property),
    custom = sum(custom),
    label621 = sum(label621)
  )
mobile_test2$High_risk <- NULL
mobile_test2$flag_1 <- NULL                 
mobile_test2$flag_3      <- NULL                     
mobile_test2$flag_5  <- NULL                        
mobile_test2$high_profit<- NULL          
mobile_test2$industry_tag <- NULL             
mobile_test2$class_elim <- NULL           
mobile_test2$p2p <- NULL                        
mobile_test2$label621 <- NULL          
mobile_test2$label464 <- NULL                
mobile_test2$label631 <- NULL                 
mobile_test2$label318 <- NULL                  
mobile_test2$label645 <- NULL                      
mobile_test2$take_away <- NULL                  
mobile_test2$video_tag <- NULL          
mobile_test2$im_tag <- NULL                        
mobile_test2$label463 <- NULL                     
mobile_test2$industry_cat <- NULL          
mobile_test2$property <- NULL          
mobile_test2$custom <- NULL           
mobile_test2$gender <- ifelse((mobile_test2$gender == 'M'), 1, 0)
#merging counts together and adding testographics back into training set
mobile_test2 <- merge(mobile_test2, by_device_label_count, by = 'device_id')
names(mobile_test2)

rm(by_device_label_count)

setkey(mobile_test2, NULL)
mobile_test2 <- unique(mobile_test2)
summary(mobile_test2)
names(mobile_test2)

mobile_all <-rbind(mobile_tr_val, mobile_test2)

head(mobile_all)
rm(mobile_val, mobile_test, mobile_dem, mobile_dem2, mobile_tr_val, mobile_test2, mobile_val2)
set.seed(1306) # set random number generator seed to enable
# repeatability of results
n <- dim(mobile_all)[1]
tr.test <- sample(n, round(n/4)) # randomly sample 33% train, validate, and test
mobile.test <- mobile_all[tr.test,]
mobile_train <- mobile_all[-tr.test,]
names(mobile_all)
################################### Scale and Center Data ###########################################

data.train <- mobile_train
x.train <- data.train[,c(5:9, 11:41, 44:50, 52:69)]
str(x.train)
c.train <- data.train[,2] # group, gender

n.train.c <- length(c.train) # 17468    
y.train <- data.train$age # age
n.train.y <- length(y.train) # 17468

data.test <- mobile.test
n.test <- dim(data.test)[1] # 5823
x.test <- data.test[,c(5:9, 11:41, 44:50, 52:69)]

x.train.mean <- apply(x.train, 2, mean)
x.train.mean
x.train.sd <- apply(x.train, 2, sd)

x.train.std <- t((t(x.train)-x.train.mean)/x.train.sd) # standardize to have zero mean and unit sd
summary(x.train.std)
apply(x.train.std, 2, mean) # check zero mean
apply(x.train.std, 2, sd) # check unit sd
data.train.std.c <- data.frame(x.train.std, gender=c.train) # to classify gender
data.train.std.y <- data.frame(x.train.std, age=y.train) # to predict age

x.test.std <- t((t(x.test)-x.train.mean)/x.train.sd) # standardize using training mean and sd
data.test.std <- data.frame(x.test.std)

#writing the final standardized gender train dataset to a csv
write_csv(data.train.std.c, "CJ.mobile.train_gender.csv")
#writing the final standardized age train dataset to a csv
write_csv(data.train.std.y, "CJ.mobile.train_age.csv")
#writing the final standardized test dataset to a csv
write_csv(data.test.std, "CJ.mobile.test.csv")

################################ Begin Building Recommender  #################################
