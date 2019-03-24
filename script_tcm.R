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
filename <- "data/cnpj/bd_tcm_pessoal.csv"
#filename <- "data/airlines/airline.csv"
sc = spark_connect(master = "local", config = config)
tic = Sys.time()
cnpj = spark_read_csv(sc, name = "cnpj", path = filename, delimiter = ";",header=FALSE)
names(cnpj) <- c("data","ano","mes","cod_municipio","nm_municipio","cod_entidade","nm_entidade","nome","matricula","data_admissao","tipo_servidor","cargo","salario_base","salario_vantagens","salario_gratificacao","decimo_terceiro","carga_horaria","area_atuacao")
(toc = Sys.time() - tic)


#### Calculate the mean monthly "salario_base" per municipality for the whole period
tic = Sys.time()
mean_salario_base = cnpj %>%
  group_by(ano, mes, nm_municipio) %>%
  summarise(mean_salario_base = mean(salario_base,na.rm=TRUE))
(toc = Sys.time() - tic)

#### Collect the data from Spark to R
tic = Sys.time()
r_mean_salario_base = collect(mean_salario_base)
(toc = Sys.time() - tic)

saveRDS(r_mean_salario_base,file="r_mean_salariobase.RDS")
library(lubridate)

#### Comparing two cities (Salvador and Lauro de Freitas)
dep_salario_base =  r_mean_salario_base %>%
  filter(toupper(nm_municipio) %in% c("LAURO DE FREITAS","SALVADOR")) %>%
  arrange(ano, mes) %>%
  mutate(date = ymd(paste(ano, mes, "01", sep = "-")))

p1 <- ggplot(dep_salario_base, aes(date, mean_salario_base, col = nm_municipio)) + geom_smooth() + xlab("Data") + ylab("Salario Base medio") + ggtitle("Salario Base medio")
png("ts_SalarioMedio.png",width=3200,height=1800,res=300)
print(p1)
dev.off()

