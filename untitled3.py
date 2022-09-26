# -*- coding: utf-8 -*-
"""
Created on Sun Sep 25 17:18:44 2022

@author: TMudastu
"""

import pyodbc
import pandas as pd
import datetime as dt
import numpy as np
import timedelta


conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=systechtraining.database.windows.net;'
                      'Database=Bootcamp;'
                      'UID=	B32022_TMudastu;'
                      'PWD=n2g9JxqKT3pYr;'
                      'Trusted_Connection=no;')



NWOrderDF = pd.read_sql_query('SELECT * FROM Northwind.Orders', conn)
NWOrderDetailsDF = pd.read_sql_query('SELECT * FROM Northwind.Order_Details', conn)
NWEmployeesDF = pd.read_sql_query('SELECT * FROM Northwind.Employees', conn)
NWCustomersDF = pd.read_sql_query('SELECT * FROM Northwind.Customers', conn)
NWShippersDF = pd.read_sql_query('SELECT * FROM Northwind.Shippers', conn)
NWProductsDF = pd.read_sql_query('SELECT * FROM Northwind.Products', conn)
NWSuppliersDF = pd.read_sql_query('SELECT * FROM Northwind.Suppliers', conn)
NWCategoriesDF = pd.read_sql_query('SELECT * FROM Northwind.Categories', conn)



Merge1 = NWOrderDF.merge(NWOrderDetailsDF, how='inner', on='OrderID') #Orders and OrderDetails
Merge1
Merge2 = Merge1.merge(NWEmployeesDF, how='inner', on='EmployeeID') #Joining Employees
Merge2
Merge3 = Merge2.merge(NWCustomersDF, how='inner', on='CustomerID') #Joining Customers
Merge3
Merge4 = Merge3.merge(NWShippersDF, how='inner', left_on='ShipVia', right_on= 'ShipperID') #Joining Shippers
Merge4
Merge5 = Merge4.merge(NWProductsDF, how='inner', on='ProductID') #Joining Products
Merge5
Merge6 = Merge5.merge(NWSuppliersDF, how='inner', on='SupplierID') #Joining Suppliers
Merge6
Merge7 = Merge6.merge(NWCategoriesDF, how='inner', on='CategoryID') #Joining Categories
Merge7



Merge7.columns



TGT_DI_CERT_FINAL = pd.DataFrame()



TGT_DI_CERT_FINAL



TGT_DI_CERT_FINAL['Employee_Name'] = (Merge7['LastName'] + ' ' + Merge7['FirstName']).replace('#','').fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Employee_Job_Desc'] = Merge7['Title'].fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Customer_Name'] = Merge7['ContactName_x'].fillna('N/A').replace('#','')



TGT_DI_CERT_FINAL['Item_Name'] = Merge7['ProductName'].fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Category_Name'] = Merge7['CategoryName'].fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Shipper_Name'] = Merge7['CompanyName'].fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Supplier_Name'] = Merge7['ContactName_y'].fillna('N/A').str.strip()



TGT_DI_CERT_FINAL['Order_ID'] = (Merge7['OrderID']*100).fillna(9999)



TGT_DI_CERT_FINAL['Order_Date_Entered'] = Merge7['OrderDate'].dt.strftime('%d-%a-%y').fillna('01-Jan-00')



TGT_DI_CERT_FINAL['Order_Date_Promised'] = Merge7['RequiredDate'].dt.strftime('%d-%a-%Y').fillna('01-Jan-00')




TGT_DI_CERT_FINAL['Order_Date_Shipped'] = Merge7['ShippedDate'].dt.strftime('%d-%m-%y').fillna('01-Jan-00')



TGT_DI_CERT_FINAL['Shipping_Days'] = ((Merge7['ShippedDate'] - Merge7['OrderDate'])/np.timedelta64(1,'D')).fillna(0).astype(int)



TGT_DI_CERT_FINAL['Item_Price'] = Merge7['UnitPrice_x'].fillna(0)



TGT_DI_CERT_FINAL['Quantity'] = sorted(Merge7['Quantity'])



TGT_DI_CERT_FINAL = TGT_DI_CERT_FINAL.groupby(['Employee_Name','Employee_Job_Desc','Customer_Name','Item_Name', 'Category_Name',
                                               'Shipper_Name', 'Supplier_Name', 'Order_ID', 'Order_Date_Entered','Order_Date_Promised',
                                               'Order_Date_Shipped','Shipping_Days', 'Item_Price','Discount', 'Sales_Without_Discount', 'Load_Date']).sum('Quantity').reset_index()


if TGT_DI_CERT_FINAL['Quantity'].all() < 2 and TGT_DI_CERT_FINAL['Item_Price'].all() < 100:
    TGT_DI_CERT_FINAL['Discount'] = Merge7['Discount'].all()+(Merge7['Discount']*0.15).all()
elif TGT_DI_CERT_FINAL['Quantity'].all() <2 and TGT_DI_CERT_FINAL['Item_Price'].all() > 100 and TGT_DI_CERT_FINAL['Quantity'].all() < 300 :
    TGT_DI_CERT_FINAL['Discount'] = Merge7['Discount']+Merge7['Discount']*0.20
elif TGT_DI_CERT_FINAL['Quantity'].all() < 3 and TGT_DI_CERT_FINAL['Item_Price'].all() > 300 and TGT_DI_CERT_FINAL['Quantity'].all() < 400:
    TGT_DI_CERT_FINAL['Discount'] = Merge7['Discount'].all()+Merge7['Discount'].all()*0.25
elif TGT_DI_CERT_FINAL['Quantity'].all() < 4 and TGT_DI_CERT_FINAL['Item_Price'].all() > 100 and TGT_DI_CERT_FINAL['Quantity'].all() < 500:
    TGT_DI_CERT_FINAL['Discount'] = Merge7['Discount'].all()+Merge7['Discount'].all()*0.30
else:
    TGT_DI_CERT_FINAL['Discount'] = Merge7['Discount'].all()+Merge7['Discount'].all()*0.35
TGT_DI_CERT_FINAL['Sales_Without_Discount'] = Merge7['UnitPrice_x']*Merge7['Quantity']

TGT_DI_CERT_FINAL['Load_Date'] = pd.Timestamp.now()
cursor = conn.cursor()

cursor.execute('CREATE TABLE TGT_DI_CERT_FINAL3(Employee_Name VARCHAR(100) NOT NULL, Employee_Job_Desc VARCHAR(100) NOT NULL,Customer_Name VARCHAR(100) NOT NULL, Item_Name VARCHAR(100) NOT NULL, Category_Name VARCHAR(100) NOT NULL, Shipper_Name VARCHAR(100) NOT NULL, Supplier_Name VARCHAR(100) NOT NULL, Order_ID int NOT NULL, Order_Date_Entered VARCHAR(100) NOT NULL, Order_Date_Promised VARCHAR(100) NOT NULL, Order_Date_Shipped VARCHAR(100) NOT NULL, Shipping_Days int NOT NULL, Item_Price money NOT NULL, Quantity int NOT NULL, Discount Float NOT NULL, [Sales_Without_Discount] money NOT NULL, Load_Date DATETIME NOT NULL)')
cursor.commit()

cursor.close()

TGT_DI_CERT_FINAL.rename(columns = {'Sales Without Discount':'Sales_Without_Discount'}, inplace = True)



conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=systechtraining.database.windows.net;'
                      'Database=Bootcamp;'
                      'UID=B32022_MSalih;'
                      'PWD=Gqsm4Ra6SfnNJ;'
                      'Trusted_Connection=no;')

cursor= conn.cursor()

for index, row in TGT_DI_CERT_FINAL.iterrows():
    cursor.execute('INSERT INTO TGT_DI_CERT_FINAL3 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', row.Employee_Name, row.Employee_Job_Desc,
    row.Customer_Name, row.Item_Name, row.Category_Name, row.Shipper_Name, row.Supplier_Name,row.Order_ID, row.Order_Date_Entered,
    row.Order_Date_Promised, row.Order_Date_Shipped, row.Shipping_Days, row.Item_Price, row.Quantity, row.Discount, row.Sales_Without_Discount,
    row.Load_Date)

cursor.commit()
cursor.close()