#R Code modified from publicly available Kaggle sources
#https://www.kaggle.com/rissonyao/talkingdata-mobile-user-demographics/talkingdata-eda-benchmarking
#install.packages("caret", dependencies = c("Depends", "Suggests"))
#install.packages("pryr")
#install.packages("devtools")
rm(list=ls())
library(DataExplorer); library(lars); library(caret); library(e1071); 
library(class);library(leaps); library(Hmisc); library(plyr); library(devtools); library(ggplot2); # Data visualization
library(readr); # CSV file I/O, e.g. the read_csv function; 
library(lubridate); library(date) # date functions
library(data.table); # Using data.table to speed up runtime
library(repr); # Use repr to adjust plot size
library(profvis); library(dplyr); library(ggmap); library(knitr);library(scales) #for scaling dates
library(reshape2); library(bit64); library(showtext);library(extrafont); library(zoo); library(dummy)
library(doParallel)
registerDoParallel(cores = 4)
require(data.table)

# load the data
gat.tr <- fread("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\gender_age_train\\gender_age_train.csv", head=T, colClasses = c("character", "character", "integer", "character"))# load the "gender, age training data" file
names(gat.tr) #"device_id" "gender"    "age"       "group" 
setkeyv(gat.tr, "device_id")

dim(gat.tr) # 74645     4
summary(gat.tr) #device_id            gender               age           group

#Creating demographic chart by age and gender
#options(repr.plot.width=6, repr.plot.height=3)
#qplot(age, data=gat.tr, facets=gender~., fill=gender)

#Loading second file brand Device Type data and setting device ID as the key
brand <- fread("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\phone_brand_device_model.csv\\phone_brand_device_model.csv", head= T, colClasses = c("character", "character", "character")) # load the "brand models" file
names(brand) #"device_id"    "phone_brand"  "device_model"
dim(brand) #187245      3

#Remove duplicates
setkey(brand,NULL)
brand <-unique(brand)
dim(brand)
setkey(brand,device_id)

brand3 <- merge(gat.tr,brand,by="device_id")

#Remove old object to save memory **Very important with dataset this large**
rm(brand,gat.tr)
head(brand3)
dim(brand3) # 74646     6
head(brand3)
summary(brand3)

#Add event data using device_id as key
events <- read.csv("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\events.csv\\events.csv", header=T, numerals='no.loss') # load the "mobile events" file 
names(events) # "event_id"  "device_id" "timestamp" "longitude" "latitude" 
dim(events) #3252950       5
summary(events)
#Remove duplicates: None found

#events$timestamp <- as.Date(events$timestamp, format=c('%Y-%m-%d H:%M:%S'))
events$event_id <- as.character(events$event_id)
events$device_id <- as.character(events$device_id)
events <- events [with(events, longitude != 0 & latitude != 0), ]

summary(events)
events <- data.table(events)
setkey(events,device_id)
dim(events) #2283959       5

gat_br_events <- merge(events, brand3, by="device_id")
#Remove old objects
rm(brand3,events)
dim(gat_br_events) # 859293     10
summary(gat_br_events)
head(gat_br_events) # device_id event_id timestamp longitude latitude gender age  group phone_brand device_model

#Change key to event ID to merge with app events
setkey(gat_br_events,event_id)


#Add app events and merge to new dataset
app_events <- fread("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\app_events.csv\\app_events.csv", colClasses = c("character", "character","numeric", "numeric"))
# load the "application events" file
names(app_events) #"event_id"     "app_id"       "is_installed" "is_active" 
setkey(app_events,event_id) #
dim(app_events) # 32473067        4
mobile2 <- merge(app_events,gat_br_events,by="event_id")

rm(gat_br_events, app_events);gc()
mobile2 <- data.table(mobile2)
summary(mobile2)
dim(mobile2) # 4958914      13
head(mobile2)

#Change key to app_id to merge app_label data
setkey(mobile2,app_id)
head(mobile2)

##Load app label data and merge with label categories before merging with mobile2
app_labels <- fread("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\app_labels.csv\\app_labels.csv", colClasses = c("character", "character")) # load the "app labels" file
names(app_labels) #"app_id"   "label_id"
setkey(app_labels,label_id) #
dim(app_labels) #459943      2
summary(app_labels)
app_labels <- data.table(app_labels)
#Load Label Cateogries
label_cat <- fread("C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\label_categories.csv\\label_categories.csv", colClasses = c("character", "character")) # load the "label categories" file
names(label_cat) #"label_id" "category"
setkey(label_cat,label_id)
dim(label_cat) #930   2
summary(label_cat) 
label_cat <- data.table(label_cat)
labels <- merge(label_cat,app_labels,by="label_id", allow.cartesian=TRUE)
rm(label_cat,app_labels)
dim(labels)
setkey(labels, NULL)
labels <- unique(labels)
dim(labels)
names(labels) #"label_id" "category" "app_id" 
setkey(labels, "app_id")
summary(labels)
#write.csv(labels, file = "C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\labels.csv")
#Check Memory allocation to see if you need to remove anything
.ls.objects <- function (pos = 1, pattern, order.by,
                         decreasing=FALSE, head=FALSE, n=5) {
  napply <- function(names, fn) sapply(names, function(x)
    fn(get(x, pos = pos)))
  names <- ls(pos = pos, pattern = pattern)
  obj.class <- napply(names, function(x) as.character(class(x))[1])
  obj.mode <- napply(names, mode)
  obj.type <- ifelse(is.na(obj.class), obj.mode, obj.class)
  obj.prettysize <- napply(names, function(x) {
    capture.output(print(object.size(x), units = "auto")) })
  obj.size <- napply(names, object.size)
  obj.dim <- t(napply(names, function(x)
    as.numeric(dim(x))[1:2]))
  vec <- is.na(obj.dim)[, 1] & (obj.type != "function")
  obj.dim[vec, 1] <- napply(names, length)[vec]
  out <- data.frame(obj.type, obj.size, obj.prettysize, obj.dim)
  names(out) <- c("Type", "Size", "PrettySize", "Rows", "Columns")
  if (!missing(order.by))
    out <- out[order(out[[order.by]], decreasing=decreasing), ]
  if (head)
    out <- head(out, n)
  out
}

lsos <- function(..., n=10) {
  .ls.objects(..., order.by="Size", decreasing=TRUE, head=TRUE, n=n)
}

lsos()

## Merge large mobile data table with labels
mobile.train <- merge(labels, mobile2, by = "app_id", allow.cartesian=TRUE)

summary(labels)
#Remove old object and check memory allocation
rm(mobile2, labels);gc()
dim(mobile.train) # 32158223       15
setkey(mobile.train, NULL)
mobile.train <-unique(mobile.train)
dim(mobile.train)
summary(mobile.train) 
head(mobile.train)

dim(mobile.train) #32158223       15

# Write CSV in R
write.csv(mobile.train, file = "C:\\Users\\user1\\Documents\\Northwestern University\\PREDICT 498\\Data\\mobile.train.csv")


