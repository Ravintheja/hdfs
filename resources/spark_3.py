#Spark Script 03 - Win, losses, drawss for each Country
print('====== Script 03 ======')

 from pyspark.sql.functions import col

hwin = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] > ftball_res["away_score"]).groupBy("home_team").count() 
alost = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] > ftball_res["away_score"]).groupBy("away_team").count()
awin = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] < ftball_res["away_score"]).groupBy("away_team").count() 
hlost = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] < ftball_res["away_score"]).groupBy("home_team").count()  
hdraw = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] == ftball_res["away_score"]).groupBy("home_team").count()
adraw = ftball_res.select("home_team", "away_team").filter(ftball_res["home_score"] == ftball_res["away_score"]).groupBy("away_team").count() 



hwin1 = hwin.select(col("home_team").alias("country_hw"), col("count").alias("hw_count"))
awin1 = awin.select(col("away_team").alias("country_aw"), col("count").alias("aw_count"))
hlost1 = hlost.select(col("home_team").alias("country_hl"), col("count").alias("hl_count"))
alost1 = alost.select(col("away_team").alias("country_al"), col("count").alias("al_count"))
hdraw1 = hdraw.select(col("home_team").alias("country_hd"), col("count").alias("hd_count"))
adraw1 = adraw.select(col("away_team").alias("country_ad"), col("count").alias("ad_count"))
	
winjoin = hwin1.join(awin1, hwin1["country_hw"] == awin1["country_aw"], how='left')
losjoin = hlost1.join(alost1, hlost1["country_hl"] == alost1["country_al"], how='left')
drawjoin = hdraw1.join(adraw1, hdraw1["country_hd"] == adraw1["country_ad"], how='left')
	
totwins = winjoin.select(col("country_hw").alias("Country_w"), (col("hw_count") + col("aw_count")).alias("Total Wins"))
totlos = losjoin.select(col("country_hl").alias("Country_l"), (col("hl_count") + col("al_count")).alias("Total Losses")).show()
totdraw = drawjoin.select(col("country_hd").alias("Country_d"), (col("hd_count") + col("ad_count")).alias("Total Draws"))
	
Final = totwins.join(totlos, totwins["Country_w"] == totlos["Country_l"], how='left')
Final1 = Final.join(totdraw, Final["Country_w"] == totdraw["Country_d"], how='left')
