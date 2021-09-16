library('tidyverse')
library('rvest')
library('RSelenium')
library("maps")

(state <- as_tibble(map_data("state")))
urls <- sapply(unique(state$region), function(x)str_c("https://www.nbcnews.com/politics/2020-elections/", str_replace_all(x," ", "-"), "-president-results"))
urls

Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.8.0_281\\bin')
rd <- rsDriver(chromever = "88.0.4324.96", verbose = F)
remdr <- rd$client
records <- vector("list", length = length(urls))

for (i in seq_along(urls)){
  remdr$navigate(urls[i])
  if (length(remdr$findElements(using = "css", ".jsx-1765211304"))!= 0){
    loadmore <- remdr$findElement(using = "css",".jsx-1765211304")
    Sys.sleep(3)
    loadmore$clickElement()
  }
  Sys.sleep(3)
  webpage <- remdr$getPageSource() %>% .[[1]] %>% read_html
  results <- html_nodes(webpage, "div.jsx-1858457747")
  if (i == 18){
    state_county <- html_nodes(results, ".jsx-1505179373 div.publico-txt") %>% html_text() %>% str_remove("100% in") %>% str_to_lower() %>% str_c(names(urls)[i], ",", .) %>% head(.,-2)
  } else if (i == 26) {
    state_county <- html_nodes(results, ".jsx-1505179373 div.publico-txt") %>% html_text() %>% str_remove("100% in") %>% str_to_lower() %>% str_c(names(urls)[i], ",", .) %>% head(.,-3)
  } else{
    state_county <- html_nodes(results, ".jsx-1505179373 div.publico-txt") %>% html_text() %>% str_remove("100% in") %>% str_to_lower() %>% str_c(names(urls)[i], ",", .) 
  }
  votes <- html_nodes(results, "div.jsx-3437879980")%>% html_text()
  if (i == 18 | i == 26) others_number <- html_nodes(results, "span.jsx-4189516194 ")%>% html_text(trim = T) %>% length()/4 -2 else others_number <- html_nodes(results, "span.jsx-4189516194 ")%>% html_text(trim = T) %>% length()/2 -2
  trump <- votes[2:(length(state_county)+1)] %>% str_remove_all(.,",") %>% as.numeric()
  biden <- votes[(length(state_county)+3):(2*length(state_county)+2)] %>% str_remove_all(.,",") %>% as.numeric()
  others <- sapply(seq_len(others_number), function(x) votes[((x+1)*length(state_county)+(3+x)): ((2+x)*length(state_county)+(2+x))] %>% str_remove_all(.,",") %>% as.numeric()) %>% rowSums()
  records[[i]] <- tibble(state_county = state_county, trump = trump, biden = biden, others = others)
}
election_res_2020 <- bind_rows(records)
election_res_2020
write.csv(election_res_2020, "election_res_2020.csv")
