

# Internal function for reading the data given filename/year
.readfun90s <- function(fname_state, yyyy, dict) {
                                        # Recover state name:
  state_recovered <- gsub(".*([a-z]{2}).\\.txt$", "\\1",
                          fname_state, ignore.case = TRUE)

  df_state <- .readfun(fname, dict)


  df_state[, .year := yyyy]
  df_state[, .state := state_recovered]

  message(glue::glue("[Year {yyyy}]     Finished state {state_recovered}"))

  return(df_state)
}

.readfun2k <- function(fname, yyyy, dict) {
  df <- .readfun(fname, dict)
  df[, .year := yyyy]
  message(glue::glue("[Year {yyyy}]     Read {fname}"))
  return(df)
}

.readfun <- function(fname, dict) {
  DT <- fread(fname, header = FALSE, sep = "\n")
  DT <- DT[, lapply(seq_len(dict[, .N]), function(ii) {
    stri_sub(V1, dict[, Start[ii]], dict[, End[ii]])
    ## https://stackoverflow.com/questions/24715894/faster-way-to-read-fixed-width-files
  })]
  setnames(DT, new = dict[, Name])
}
