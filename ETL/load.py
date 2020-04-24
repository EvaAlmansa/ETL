import pandas as pd


class Load:
    def __init__(self, engine):
        self.engine = engine

    def load(self, output_path):
        merchant = pd.read_sql_table('merchant', con=self.engine)
        industry = pd.read_sql_table('industry', con=self.engine)
        monthlydistribution = pd.read_sql_table('monthlydistribution', con=self.engine)

        merchant = merchant.set_index('merchant_uuid')
        industry = industry.set_index('industry_code')

        merchant[['count', 'sum_booking']].to_excel(output_path + "merchant.xlsx")
        industry[['count', 'sum_booking']].to_excel(output_path + "industry.xlsx")
        monthlydistribution[['month', 'industry_code']].to_excel(output_path + "monthlydistribution.xlsx", index=False)
