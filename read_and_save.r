library(data.table)
library(iotools)
library(stringi)

## I used `PME_1991_a_2000/pme1991-2000.doc` to create the dictionaries in the `input` directory

ldict <- list(
  person = data.table::fread("input/dict-90-2k-person-treated.csv"),
  household = data.table::fread("input/dict-90-2k-household-treated.csv")
)

# Include `End` column
invisible(lapply(ldict, function(DT) DT[, End := Start + Width - 1]))


# Start dealing with PME data...
zip_dir <- "/home/gustavo/Dropbox/v2/data/PME/PME_1991_a_2000/TXT"
tmpd <- tempdir()

out_dir <- "/home/gustavo/Dropbox/v2/data/PME/FullData"

# Internal function for reading the data given filename/year
.readfun2 <- function(fname_state, yyyy, dict) {
                                        # Recover state name:
  state_recovered <- gsub(".*([a-z]{2}).\\.txt$", "\\1",
                          fname_state, ignore.case = TRUE)

  df_state <- fread(fname_state, header = FALSE, sep = "\n")
  df_state <- df_state[, lapply(seq_len(dict[, .N]), function(ii) {
    stri_sub(V1, dict[, Start[ii]], dict[, End[ii]])
    ## https://stackoverflow.com/questions/24715894/faster-way-to-read-fixed-width-files
  })]

  setnames(df_state, new = dict[, Name])

  df_state[, .year := yyyy]
  df_state[, .state := state_recovered]

  message(glue::glue("[Year {yyyy}]     Finished state {state_recovered}"))

  return(df_state)
}


# Internal function for reading the data given filename/year
.readfun <- function(fname_state, yyyy) {
                                        # Recover state name:
  state_recovered <- gsub(".*([a-z]{2}).\\.txt$", "\\1",
                          fname_state, ignore.case = TRUE)

  df_state <- iotools::input.file(
                         fname_state, formatter = dstrfw,
                         col_types = rep("character", nrow(ldict$person)),
                         widths = ldict$person[, Width]
                       )

  df_state[, .year := yyyy]
  df_state[, .state := state_recovered]

  message(glue::glue("[Year {yyyy}]     Finished state {state_recovered}"))

  return(df_state)
}



# Start reading stuff
for (yyyy in as.character(seq(1991, 2000, by = 1))) {
  message(glue::glue("[Year {yyyy}] Starting"))

  zipf <- glue::glue("pme{yyyy}.zip")

  u <- unzip(file.path(zip_dir, zipf), exdir = tmpd)


  datasets_person <- grep("p\\.txt$", u, value = TRUE, ignore.case=TRUE)
  datasets_hh <- grep("d\\.txt$", u, value = TRUE, ignore.case=TRUE)
                                        # "d" stands for "domicilio", or
                                        # "household" in portuguese

  message(glue::glue("[Year {yyyy}] Reading person datasets"))
  dt_person <- rbindlist(lapply(datasets_person, .readfun2, yyyy = yyyy,
                                dict = ldict$person))
  message(glue::glue("[Year {yyyy}] Reading household datasets"))
  dt_hh <- rbindlist(lapply(datasets_hh, .readfun2, yyyy = yyyy,
                            dict = ldict$household))


  fwrite(dt_person, file.path(out_dir,
                              glue::glue("person-{yyyy}.csv")))

  fwrite(dt_hh, file.path(out_dir,
                          glue::glue("household-{yyyy}.csv")))


  message(glue::glue("[Year {yyyy}] Wrote parsed datasets"))
  file.remove(u)  # remove temporary files
  message(glue::glue("[Year {yyyy}] Removed temporary files in {tmpd} "))
}


yyyy <- 1995
