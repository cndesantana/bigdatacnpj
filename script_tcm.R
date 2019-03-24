library(sparklyr)
library(tidyverse)

#To define the directory where temporary files will be created by spark
#The directory should be placed in a drive with enough disk space. Otherwise the following error message might appear:
#java.io.IOException: No space left on device
spark_dir = "/home/charles/LinkedIn/spark"
setwd(spark_dir)
config = spark_config()
config$`sparklyr.shell.driver-java-options` <-  paste0("-Djava.io.tmpdir=", "tmpdir")

#If you get the following error
#java.lang.OutOfMemoryError: Java heap space
#You should add extra command lines
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "512"

#As I am working in a local machine, I connect in a "local" spark instance
filename <- "data/cnpj/airline.csv"
sc = spark_connect(master = "local", config = config)
air = spark_read_csv(sc, name = "air", path = filename)

tic = Sys.time()
mean_dep_delay = air %>%
  group_by(Year, Month, DayofMonth) %>%
  summarise(mean_delay = mean(DepDelay,na.rm=TRUE))
(toc = Sys.time() - tic)

# Source:   lazy query [?? x 4]
# Database: spark_connection
# Groups:   YEAR, MONTH
#   YEAR MONTH DAY_OF_MONTH mean_delay
#  <int> <int>        <int>      <dbl>
#1  1987    10            9       6.71
#2  1987    10           10       3.72
#3  1987    10           12       4.95
#4  1987    10           14       4.53
#5  1987    10           23       6.48
#6  1987    10           29       5.77
#Warning messages:
#1: Missing values are always removed in SQL.
#Use `AVG(x, na.rm = TRUE)` to silence this warning
#2: Missing values are always removed in SQL.
#Use `AVG(x, na.rm = TRUE)` to silence this warning

#Surprisingly, this takes around 5 minutes to print? Why? Look at the class of mean_dep_delay: it’s a lazy query that only gets evaluated once I need it. Look at the first line; lazy query [?? x 4]. This means that I don’t even know how many rows are in mean_dep_delay! The contents of mean_dep_delay only get computed once I explicitly ask for them. I do so with the collect() function, which transfers the Spark object into R’s memory:

#saveRDS(mean_dep_delay, "outputs/cnpj/mean_dep_delay.rds")
#mean_dep_delay <- readRDS("mean_dep_delay.rds")

tic = Sys.time()
r_mean_dep_delay = collect(mean_dep_delay)
(toc = Sys.time() - tic)

library(lubridate)

dep_delay =  r_mean_dep_delay %>%
  arrange(Year, Month, DayofMonth) %>%
  mutate(date = ymd(paste(Year, Month, DayofMonth, sep = "-")))

p1 <- ggplot(dep_delay, aes(date, mean_delay)) + geom_smooth() + xlab("Date") + ylab("Mean delay (min)") + ggtitle("Airlines in the US -- Mean delay")
png("ts_MeanDelay.png",width=3200,height=1800,res=300)
print(p1)
dev.off()

