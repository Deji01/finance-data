from selenium import webdriver
from selenium.webdriver.common.by import By
from rich import print

options = webdriver.ChromeOptions()
driver = webdriver.Remote(command_executor="http://51.141.238.113:4444/wd/hub", options=options)

url = "https://in.finance.yahoo.com/topic/stock-market-news/"
driver.get(url)

divs = driver.find_elements(By.CLASS_NAME, "Ov(h) Pend(44px) Pstart(25px)")
print(divs)