package problem1.com.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App
{
	public static void main(String[] args)
	{
		SparkSession sparkSession = SparkSession.builder().master("local").appName("Problem1").config("","").getOrCreate();
		
		loadProductData(sparkSession);
		loadCompetitorData(sparkSession);
		loadSellerData(sparkSession);
		
		Dataset<Row> productDFSQL = sparkSession.sql("select * from product where productId == 82545658");
		productDFSQL.show();
		
		
		Dataset<Row> competitorDFSQL = sparkSession.sql("select productId, min(price) as minPrice from competitor group by productId");
		competitorDFSQL.show();
		
		Dataset<Row> sellerDFSQL = sparkSession.sql("select * from seller where SellerID == 9001");
		sellerDFSQL.show();
		
		Dataset<Row> result = sparkSession.sql("select c.productid, case "
				+ "when p.procuredValue+p.maxMargin < min(c.price) then p.procuredValue+p.maxMargin "
				+ "when  p.procuredValue+p.minMargin < min(c.price) then min(c.price) "
				+ "when p.procuredValue < min(c.price) and c.saleEvent = 'Special' then min(c.price) "
				+ "when p.procuredValue < min(c.price) and c.saleEvent = 'Special' and s.netValue = 'VeryHigh' "
				+ "then 0.9*p.procuredValue else p.procuredValue end as finalPrice,c.fetchTS,min(c.price) Q,c.rivalName "
				+ "from product p "
				+ "join competitor c on p.ProductId = c.productId "
				+ "join seller s on p.SellerID = s.SellerID "
				+ "group by c.productId,p.procuredValue,p.maxMargin,minMargin,c.fetchTS,c.rivalName,c.saleEvent,s.netValue");
		
		result.show();
	}

	private static void loadSellerData(SparkSession sparkSession)
	{
		Dataset<Row> sellerDF = sparkSession.read().option("delimiter", "|").option("header", "true").option("inferSchema", "true")
				   .csv("C:\\Users\\mohammad.abdullah\\Downloads\\dataset\\seller_data.txt");
		sellerDF.createOrReplaceTempView("seller");
	}

	private static void loadCompetitorData(SparkSession sparkSession)
	{
		Dataset<Row> competitorDF = sparkSession.read().option("delimiter", "|").option("header", "true").option("inferSchema", "true")
				   .csv("C:\\Users\\mohammad.abdullah\\Downloads\\dataset\\Competitor_data.txt");
		competitorDF.createOrReplaceTempView("competitor");
	}

	private static void loadProductData(SparkSession sparkSession)
	{
		Dataset<Row> productDF = sparkSession.read().option("delimiter", "|").option("header", "true").option("inferSchema", "true")
		   .csv("C:\\Users\\mohammad.abdullah\\Downloads\\dataset\\internal_product_data.txt");
		productDF.createOrReplaceTempView("product");
	}
}
