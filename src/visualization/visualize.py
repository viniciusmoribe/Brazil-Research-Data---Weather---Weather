import matplotlib.pyplot as plt

def plot_histogram(df, column):
    df[column].hist()
    plt.title(f"Histograma de {column}")
    plt.show()
