library(readr)
library(dplyr)
library(ggplot2)
library(lubridate)

# load data
sales<-read_csv("../data/sales.csv")
details<-read_csv("../data/details.csv")
features<-read_csv("../data/features.csv")

# helper functions
dedollar<-function(x){ as.numeric(gsub("\\$|,","",x)) }

# clean sales
sales<-sales %>% 
  mutate(sale_price=dedollar(sale_price),
         total_assessment=dedollar(total_assessment)
         )
sales_clean<-write_csv(sales,"../data/sales_clean.csv")

# clean features
names(features)<-c("half_baths",
                   "ac",
                   "baths",
                   "beds",
                   "bld_style",
                   "bld_type",
                   "ext_cond",
                   "fireplace",
                   "floor",
                   "heat",
                   "int_cond",
                   "living_sqft",
                   "overall_cond",
                   "total_rooms",
                   "wall",
                   "year_built",
                   "ssl")
write_csv(features,"../data/features_clean.csv")

# clean details
names(details)<-c("assessor",
                   "class3_exempt",
                   "gross_bld_area",
                   "homestead_status",
                   "improvements",
                   "instrument_no",
                   "land_value",
                   "land_area",
                   "mailing_addr",
                   "neighborhood",
                   "owner_name",
                   "record_date",
                   "sale_price",
                   "sale_code",
                   "sale_type",
                   "sub_neighborhood",
                   "tax_class",
                   "tax_type",
                   "taxable_assessment",
                   "total_value",
                   "triennial_group",
                   "use_code_desc",
                   "ward",
                   "ssl")
write_csv(details,"../data/details_clean.csv")


