library(data.table)
library(stringi)


source("lib.r", chdir = TRUE)


parameters <- list(
  start = 1991,  # >= 1991
  end = 2000,    # <= 2000
  zip_dir1 =  "/home/gustavo/Dropbox/v2/data/PME/PME_1991_a_2000/TXT",
  zip_dir2 =  "/home/gustavo/Dropbox/v2/data/PME/PME_post2k",
  dir_2001 =  "/home/gustavo/Dropbox/v2/data/PME/PME_2001/2001/Dados",
  out_dir = "/home/gustavo/Dropbox/v2/data/PME/FullData"
)


## I used `PME_1991_a_2000/pme1991-2000.doc` to create the dictionaries in the `input` directory
ldict <- list(
  person = data.table::fread("input/dict-90-2k-person-treated.csv"),
  household = data.table::fread("input/dict-90-2k-household-treated.csv")
)

# Include `End` column
invisible(lapply(ldict, function(DT) DT[, End := Start + Width - 1]))


# Start dealing with PME data...
tmpd <- tempdir()


# Start reading stuff
pre2k <- intersect(seq(1991, 2000, by = 1),
                   seq(parameters$start, parameters$end, by = 1))

for (yyyy in as.character(pre2k)) {
  message(glue::glue("[Year {yyyy}] Starting"))

  zipf <- glue::glue("pme{yyyy}.zip")

  u <- unzip(file.path(parameters$zip_dir1, zipf), exdir = tmpd)


  datasets_person <- grep("p\\.txt$", u, value = TRUE, ignore.case=TRUE)
  datasets_hh <- grep("d\\.txt$", u, value = TRUE, ignore.case=TRUE)
                                        # "d" stands for "domicilio", or
                                        # "household" in portuguese

  message(glue::glue("[Year {yyyy}] Reading person datasets"))
  dt_person <- rbindlist(lapply(datasets_person, .readfun90s, yyyy = yyyy,
                                dict = ldict$person))
  message(glue::glue("[Year {yyyy}] Reading household datasets"))
  dt_hh <- rbindlist(lapply(datasets_hh, .readfun, yyyy = yyyy,
                            dict = ldict$household))


  fwrite(dt_person, file.path(parameters$out_dir,
                              glue::glue("person-{yyyy}.csv")))

  fwrite(dt_hh, file.path(parameters$out_dir,
                          glue::glue("household-{yyyy}.csv")))


  message(glue::glue("[Year {yyyy}] Wrote parsed datasets"))
  file.remove(u)  # remove temporary files
  message(glue::glue("[Year {yyyy}] Removed temporary files in {tmpd} "))
}



dict_post2k <- data.table::fread("input/dict-2k-2015.csv", colClasses = list(character = "Width"))
dict_post2k[grepl("\\.\\d+", Width), Decimal := gsub(".*\\.(\\d+)", "\\1", Width)]
dict_post2k[, End :=  Start + Width - 1]

## Read 2001
if (data.table::inrange(2001, parameters$start, parameters$end)) {
  yyyy <- "2001"

  f01 <- list.files(parameters$dir_2001)

  ldt <- lapply(f01,
                function(f) .readfun2k(file.path(parameters$dir_2001, f),
                                       "2001",
                                       dict_post2k))

  fwrite(rbindlist(ldt), file.path(parameters$out_dir,
                                   glue::glue("full-{yyyy}.csv")))

  message(glue::glue("[Year {yyyy}] Wrote parsed datasets"))
}


