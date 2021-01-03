#!/usr/bin/Rscript
# !diagnostics off

sort_list_by_name <- function(x){
  x %>% names %>% order %>% x[.]
}

my_exclude <- function(list, names){
  ## return the elements of the list not belonging to names
  member.names <- names(list)
  index <- which(!(member.names %in% names))
  list[index]    
}

`%||%` <- function(lhs, rhs){
  if (!is.null(lhs)){
    lhs
  } else {
    rhs
  }
}


remove_empty_list_elements <- function(Lst){
  # This function does what is says on the tin. 
  remove <- vapply(Lst, is.null, FUN.VALUE = logical(1))
  Lst[!remove]
}





get_mode <- function(v) {
  # This function calculates the mode of a numeric vector.
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}


dt_col_rename <- function(dt, old, new){
  # This function renames columns in a data table. 
  # This is mostly useful when changing between "." and "_" in R and databases.
  setnames(dt, gsub(old, new, names(dt)))
}


check_if_nested_df_list_is_empty <- function(object){
  # This function is inteded if nested lists of data frames or data tables are 
  # entirely empty, i.e. either contain only NULLs or data frames/tables with zero
  # rows. 
  
  # Check if object is NULL.
  if(is.null(object)){return(TRUE)}
  
  # Check if object is data frame and if so if it has any rows.
  if(is.data.frame(object)){
    if(identical(nrow(object), 0L)){
      return(TRUE) 
    } else {
      return(FALSE)}
  }
  
  # Check if object is general list.
  if(is.list(object)){
    if(identical(length(object), 0L)){
      return(TRUE) 
    } else {
      return(all(vapply(object, check_if_nested_df_list_is_empty, FUN.VALUE=logical(1))))}
  }
  
  if(identical(length(object), 0L)){return(TRUE)}
  # We found that object is neither a list nor a data frame.
  stop("There is an unexpected item in this object! There should only be lists and dataframes!")
}


df.empty <- data.frame()
dt.empty <- data.table()
df.full <- data.frame(example = c(1:10))
Lst.1 <- list(a=list(a=list(), b=list(a=NULL), d=NULL), b=list(a=list(), b=df.empty), d=list(NULL))
Lst.2 <- list(a=list(a=list(), b=list(a=NULL), d=NULL), b=list(a=list(), b=df.empty), d=list(NULL))
Lst.3 <- list(a=list(a=list(), b=list(a=NULL), d=NULL), b=list(a=list(), b=df.empty), d=list(NULL))
Lst.4 <- list(a=list(a=list(), b=list(a=NULL), d=NULL), b=list(a=list(), b=df.empty, d=df.full), d=list(NULL))

stopifnot(exprs={
  identical(check_if_nested_df_list_is_empty(NULL), TRUE)
  identical(check_if_nested_df_list_is_empty(list()), TRUE)
  identical(check_if_nested_df_list_is_empty(Lst.1), TRUE)
  identical(check_if_nested_df_list_is_empty(Lst.2), TRUE)
  identical(check_if_nested_df_list_is_empty(Lst.3), TRUE)
  identical(check_if_nested_df_list_is_empty(Lst.4), FALSE)
})


roll_function <- function(dt, func){
  # This function applies an input function over every two consecutive rows of 
  # a data table. I.e. the function func will be applied to row1 and row2, then
  # row2 and row3 and so on. 
  first.rows <- copy(dt[1:.N-1])
  first.rows <- split(first.rows, 1:nrow(first.rows))
  second.rows <- copy(dt[2:.N])
  second.rows <- split(second.rows, 1:nrow(second.rows))
  
  output <- mapply(func,
                   row.1=first.rows,
                   row.2=second.rows)
  return(output)
}
