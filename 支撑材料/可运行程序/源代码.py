import os
import json

from py2neo import Graph,Node
# https://github.com/liuhuanyong/ChainKnowledgeGraph

## 关系查询表
## match(n:company)-[r:`主营产品`]-(m:product) return n,m

class MedicalGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        # 实体列表
        # company是上市公司，是我国重要的公司代表与行业标杆，选取上市公司作为基础实体
        # 从中选取与半导体及集成电路行业有关的上市公司
        # company包括企业名name,企业全称fullname,企业编号code,上市地点及时间location,time
        self.company_path = os.path.join(cur_dir, 'data/company.json')

        # industry是行业分类，是承载企业、公司、产品的媒介
        # --行业指数和热点行业等指标
        # 对上市公司进行了行业归属
        # 选取申万发布了2021版的行业分类规范
        self.industry_path = os.path.join(cur_dir, 'data/industry.json')

        # 产品，代表公司的主营范围，用于定位公司性质
        # 其数据可以从公司的经营范围、年报等文本中进行提取得到。
        self.product_path = os.path.join(cur_dir, 'data/product.json')

        # 关系列表
        # 公司所属的行业--通过公开的上市公司行业分类表，可以得到上市公司所对应的行业分类数据。
        self.company_industry_path = os.path.join(cur_dir, 'data/company_industry.json')

        # 公司主营产品关系
        # 上市公司的经营产品数据可以从两个方面来获得，
        # 一个是从公司简介中的经营范围中结合制定的规则进行提取，
        # 另一个是从公司每年发布的半年报、年报中进行提取。
        # 这些报告中会有按经营业务、经营产品、经营地域等几个角度对公司的营收占比进行统计，
        # 也可以通过制定规则的方式进行提取。
        # 第二种方法中，由于已经有统计数据，所以我们可以根据占比数据大小，对主营产品这一关系进行赋值。
        self.company_product_path = os.path.join(cur_dir, 'data/company_product.json')

        # 行业上级关系：通过公开的行业三级分类情况，可以通过组合的形式得到行业之间的上级关系数据。
        # 即大行业下面对小行业的细分
        self.industry_industry = os.path.join(cur_dir, 'data/industry_industry.json')

        #
        self.product_product = os.path.join(cur_dir, 'data/product_product.json')
        #
        self.g = Graph("http://localhost:7474", auth=("neo4j", "1"))

    '''建立节点'''
    def create_node(self, label, nodes):
        count = 0
        for node in nodes:
            bodies = []
            for k, v in node.items():
                body = k + ":" + "'%s'"% v
                bodies.append(body)
            query_body = ', '.join(bodies)
            try:
                sql = "CREATE (:%s{%s})"%(label, query_body)
                self.g.run(sql) # 在图中执行代码，创建节点
                count += 1 # 计数加一
            except:
                pass
            print(count, len(nodes))
        return 1



    """加载数据"""
    def load_data(self, filepath):
        datas = []
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not obj:
                    continue
                datas.append(obj)
        return datas



    '''创建知识图谱实体节点类型schema'''
    def create_graphnodes(self):
        # 读取公司实体
        company = self.load_data(self.company_path)
        # 读取产品实体
        product = self.load_data(self.product_path)
        industry = self.load_data(self.industry_path)
        self.create_node('company', company)
        print(len(company))
        self.create_node('product', product)
        print(len(product))
        self.create_node('industry', industry)
        print(len(industry))
        return

    '''创建实体关系边'''
    def create_graphrels(self):
        # 创建关系时，考虑了公司所属的行业
        company_industry = self.load_data(self.company_industry_path)
        # 公司的主营产品
        company_product = self.load_data(self.company_product_path)
        #
        product_product = self.load_data(self.product_product)
        industry_industry = self.load_data(self.industry_industry)
        self.create_relationship('company', 'industry', company_industry, "company_name", "industry_name")
        self.create_relationship('industry', 'industry', industry_industry, "from_industry", "to_industry")
        self.create_relationship_attr('company', 'product', company_product, "company_name", "product_name")
        self.create_relationship('product', 'product', product_product, "from_entity", "to_entity")


    '''创建实体关联边'''
    def create_relationship(self, start_node, end_node, edges, from_key, end_key):
        count = 0
        for edge in edges:
            try:
                p = edge[from_key]
                q = edge[end_key]
                rel = edge["rel"]
                query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s]->(q)" % (
                start_node, end_node, p, q, rel)
                self.g.run(query)
                count += 1
                print(rel, count, all)
            except Exception as e:
                print(e)
        return


    '''创建实体关联边'''
    def create_relationship_attr(self, start_node, end_node, edges, from_key, end_key):
        count = 0
        for edge in edges:
            p = edge[from_key]
            q = edge[end_key]
            rel = edge["rel"]
            weight = edge["rel_weight"]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{%s:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel, "权重", weight)
            try:
                self.g.run(query)
                count += 1
                print(rel, count)
            except Exception as e:
                print(e)
        return



if __name__ == '__main__':
    handler = MedicalGraph() # 创建MedocalGraph类的对象
    handler.create_graphnodes() #
    handler.create_graphrels()
