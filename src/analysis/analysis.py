import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


class DataAnalysis:
    def __init__(self, product_keywords_path, product_rankings_path):
        self.product_keywords_path = product_keywords_path
        self.product_rankings_path = product_rankings_path
        self.merged_data = None

    # 데이터 가져옴
    def load_and_preprocess(self):
        product_keywords = pd.read_csv(self.product_keywords_path)
        product_rankings = pd.read_csv(self.product_rankings_path)
        self.merged_data = pd.merge(product_rankings, product_keywords, on="productId")
        self.merged_data["date"] = pd.to_datetime(self.merged_data["date"])
        self.merged_data = self.merged_data.sort_values(by="date", ascending=False).drop_duplicates(subset=["productId"])
        self.merged_data["recommend"] = self.merged_data["ranking"].apply(lambda x: 1 if x <= 150 else 0)
        self.merged_data["keywords"] = self.merged_data["keywords"].str.split(", ")
        self.merged_data["conversion_rate"] = self.merged_data["totalSales"] / self.merged_data["views"]

    # 키워드 빈도 시각화(추천 영역에서 최빈값, 비추천 영역에서 최빈값)
    def plot_keyword_frequency(self):
        keywords_exploded = self.merged_data.explode("keywords")
        keyword_counts = (
            keywords_exploded.groupby(["category", "recommend", "keywords"])
            .size()
            .reset_index(name="count")
            .sort_values(by=["category", "recommend", "count"], ascending=[True, True, False])
            .groupby(["category", "recommend"])
            .head(20)
        )
        categories = keyword_counts["category"].unique()
        font_path = "C:/Windows/Fonts/malgun.ttf"
        font_prop = fm.FontProperties(fname=font_path)
        plt.rc('font', family=font_prop.get_name())

        plt.figure(figsize=(12, 8))
        for i, category in enumerate(categories, 1):
            category_data = keyword_counts[keyword_counts["category"] == category]
            plt.subplot(2, 2, i)
            for recommend_value in [0, 1]:
                data = category_data[category_data["recommend"] == recommend_value]
                plt.barh(data["keywords"], data["count"], label=f"Recommend: {recommend_value}", alpha=0.7)
            plt.title(f"Category: {category}")
            plt.xlabel("Keyword Frequency")
            plt.ylabel("Keywords")
            plt.legend()
        plt.tight_layout()
        plt.show()

    # 각 특성의 중요도 분석
    def analyze_feature_importance(self):
        analysis_data = self.merged_data.drop(columns=["category", "keywords", "date", "productId", "ranking"])
        X = analysis_data.drop(columns=["recommend"])
        y = analysis_data["recommend"]

        numeric_features = X.select_dtypes(include=["int64", "float64"]).columns
        categorical_features = X.select_dtypes(include=["object"]).columns

        numeric_transformer = Pipeline(steps=[("imputer", SimpleImputer(strategy="mean")), ("scaler", StandardScaler())])
        categorical_transformer = Pipeline(steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore"))
        ])
        preprocessor = ColumnTransformer(transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features)
        ])
        model = Pipeline(steps=[
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(random_state=42))
        ])
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        model.fit(X_train, y_train)

        feature_names = (
            list(numeric_features) +
            list(model.named_steps["preprocessor"].named_transformers_["cat"].named_steps["onehot"].get_feature_names_out(categorical_features))
        )
        importances = model.named_steps["classifier"].feature_importances_
        importance_df = pd.DataFrame({
            "Feature": feature_names,
            "Importance": importances
        }).sort_values(by="Importance", ascending=False)

        plt.figure(figsize=(10, 8))
        plt.barh(importance_df["Feature"].head(10), importance_df["Importance"].head(10), color="skyblue")
        plt.xlabel("Importance")
        plt.ylabel("Feature")
        plt.title("Top 10 Features Impacting Recommend")
        plt.gca().invert_yaxis()
        plt.show()

    # 추천 여부에 따른 조회수 비교
    def analyze_views_by_recommend(self):
        categories = self.merged_data["category"].unique()
        results = []
        for category in categories:
            category_data = self.merged_data[self.merged_data["category"] == category]
            views_mean_by_recommend = category_data.groupby("recommend")["views"].mean()
            ratio = views_mean_by_recommend.get(0, 0) / views_mean_by_recommend.get(1, 0) if 1 in views_mean_by_recommend and 0 in views_mean_by_recommend else None
            results.append({
                "Category": category,
                "Views_Mean_1": views_mean_by_recommend.get(1, 0),
                "Views_Mean_0": views_mean_by_recommend.get(0, 0),
                "Ratio (Views_Mean_0/Views_Mean_1)": ratio
            })
        results_df = pd.DataFrame(results)
        print(results_df)

    # 추천 여부에 따른 판매량 비교
    def analyze_conversion_rate_by_recommend(self):
        categories = self.merged_data["category"].unique()
        results = []
        for category in categories:
            category_data = self.merged_data[self.merged_data["category"] == category]
            conversion_mean_by_recommend = category_data.groupby("recommend")["totalSales"].mean()
            ratio = conversion_mean_by_recommend.get(0, 0) / conversion_mean_by_recommend.get(1, 0) if 1 in conversion_mean_by_recommend and 0 in conversion_mean_by_recommend else None
            results.append({
                "Category": category,
                "TotalSales_Mean_1": conversion_mean_by_recommend.get(1, 0),
                "TotalSales_Mean_0": conversion_mean_by_recommend.get(0, 0),
                "Ratio (TotalSales_Mean_0/TotalSales_Mean_1)": ratio
            })
        results_df = pd.DataFrame(results)
        print(results_df)


def main():
    product_keywords_path = '../../data/processed/product_keywords.csv'
    product_rankings_path = '../../data/processed/product_rankings.csv'
    analysis = DataAnalysis(product_keywords_path, product_rankings_path)
    analysis.load_and_preprocess()
    analysis.plot_keyword_frequency()
    analysis.analyze_feature_importance()
    analysis.analyze_views_by_recommend()
    analysis.analyze_conversion_rate_by_recommend()


if __name__ == "__main__":
    main()
