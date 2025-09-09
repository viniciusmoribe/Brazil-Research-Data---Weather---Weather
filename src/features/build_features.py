def add_features(df):
    df['nova_feature'] = df['coluna1'] / df['coluna2']
    return df
