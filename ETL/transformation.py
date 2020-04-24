from extractions import Extractions
from model import Industry, IndustryCode, Merchant, MonthlyDistribution
import pandas as pd


class Transformation(Extractions):

    def __init__(self, url, output_path, name_db):
        super().__init__(url, output_path, name_db)

    def _count_sum(self, data, column):
        count_booking = pd.DataFrame(self.orders.groupby([column]).count()['booking'])
        sum_booking = pd.DataFrame(self.orders.groupby([column]).sum()['booking'])
        count_booking.columns=['count']
        sum_booking.columns=['sum']
        return pd.concat([count_booking,sum_booking], axis=1)

    def _sum_industry(self, data, columns):
        sum_booking = pd.DataFrame(self.orders.groupby(columns).sum()['booking'])
        sum_booking.columns=['sum']
        return sum_booking

    def _init_industry_code(self):
        for industry_code in list(self.merchants.industry_code.unique()):
            industry_code = IndustryCode(industry_code=industry_code)
            self.session.add(industry_code)
        self.session.commit()

    def _init_industry(self):
        result = self._count_sum(self.orders, 'merchant_uuid')

        for index, row in result.iterrows():
            industry = self.session.query(IndustryCode).filter(IndustryCode.industry_code == index).first()
            merchant = Merchant(
                merchant_uuid=index, count=row['count'], sum_booking=row['sum'], industry=industry
            )
            self.session.add(merchant)
        self.session.commit()

    def _init_merchant(self):

        def filter_uuid(x):
            return list(self.merchants[x == self.merchants['uuid']]['industry_code'])[0]
        self.orders['industry_code'] = self.orders['merchant_uuid'].apply(filter_uuid)

        result = self._count_sum(self.orders, 'industry_code')

        for index, row in result.iterrows():
            industry = self.session.query(IndustryCode).filter(IndustryCode.industry_code == index).first()
            industry = Industry(
                count=row['count'], sum_booking=row['sum'], industry=industry
            )
            self.session.add(industry)
        self.session.commit()

    def _init_monthlydistribution(self):
        self.orders['Month'] = self.orders.created.dt.month

        result = self._sum_industry(self.orders, ['Month', 'industry_code'])

        for index, row in result.iterrows():
            industry = self.session.query(IndustryCode).filter(IndustryCode.industry_code == index[1]).first()
            monthly = MonthlyDistribution(
                month=index[0], sum_booking=row['sum'], industry=industry
            )
            self.session.add(monthly)
        self.session.commit()

    def transformation(self):
        self._init_industry_code()
        self._init_industry()
        self._init_merchant()
        self._init_monthlydistribution()
