from matplotlib import scale
import seaborn as sns
import pandas as pd
import pathlib
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker as mticker

matplotlib.use("TkAgg")


LOG_DIR = pathlib.Path(__file__).parent / "bench_logs"
IMAGE_DIR = LOG_DIR / "images"


def plot_bench_1_throughput(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_1.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    sns_plot = sns.lineplot(x=df["remote_percentage"], y=df["total_throughput"], ax=ax)
    sns_plot.set(xlabel="Процент нелокальных запросов", ylabel="Пропускная способность оп/с")

    sns_plot.set_xticks(df["remote_percentage"])
    sns_plot.set_xticklabels([f"{p}%" for p in df["remote_percentage"]])
    sns_plot.set_yticks([p * 10 ** 6 for p in range(10)])
    sns_plot.set_yticklabels([f"{p}M" for p in range(10)])
    sns_plot.set_title("Пропускная способность при нагрузке 100% операций чтения")
    plt.savefig(image_dir / "bench_1_throughput.png")


def plot_bench_1_mean_latency(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_1.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    sns_plot = sns.lineplot(x=df["remote_percentage"], y=df["mean"], ax=ax)
    sns_plot.set(xlabel="Процент нелокальных запросов", ylabel="Медианная задержка µs")

    sns_plot.set_xticks(df["remote_percentage"])
    sns_plot.set_xticklabels([f"{p}%" for p in df["remote_percentage"]])
    sns_plot.set_yticks([p for p in range(1, 7)])
    sns_plot.set_yticklabels([f"{p} µs" for p in range(1, 7)])
    sns_plot.set_title("Медианная задержка при нагрузке 100% операций чтения")
    plt.savefig(image_dir / "bench_1_mean_latency.png")


def plot_bench_2_throughput(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_2.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    sns_plot = sns.lineplot(x=df["remote_percentage"], y=df["total_throughput"], ax=ax)
    sns_plot.set(xlabel="Процент нелокальных запросов", ylabel="Пропускная способность оп/с")

    sns_plot.set_xticks(df["remote_percentage"])
    sns_plot.set_xticklabels([f"{p}%" for p in df["remote_percentage"]])
    sns_plot.set_yticks([p * 10 ** 5 for p in range(3, 9)])
    sns_plot.set_yticklabels([f"{p*100}K" for p in range(3, 9)])
    sns_plot.set_title("Пропускная способность при нагрузке 100% операций записи")
    plt.savefig(image_dir / "bench_2_throughput.png")


def plot_bench_2_mean_latency(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_2.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    sns_plot = sns.lineplot(x=df["remote_percentage"], y=df["mean"], ax=ax)
    sns_plot.set(xlabel="Процент нелокальных запросов", ylabel="Медианная задержка µs")

    sns_plot.set_xticks(df["remote_percentage"])
    sns_plot.set_xticklabels([f"{p}%" for p in df["remote_percentage"]])
    sns_plot.set_yticks([p for p in range(4, 19)])
    sns_plot.set_yticklabels([f"{p} µs" for p in range(4, 19)])
    sns_plot.set_title("Медианная задержка при нагрузке 100% операций записи")
    plt.savefig(image_dir / "bench_2_mean_latency.png")


def plot_bench_3(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_3.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    x = ['Чтение 70% Запись 30%', 'Чтение 30% Запись 70%']
    y = df['total_throughput'][:2]
    sns_plot = sns.barplot(x=x, y=y, ax=ax)
    sns_plot.set(xlabel="Используемый тип хранилища", ylabel="Пропускная способность оп/с")

    # sns_plot.set_xticks(df["remote_percentage"])
    # sns_plot.set_xticklabels([f"{p}%" for p in df["remote_percentage"]])
    # sns_plot.set_yticks([p * 10 ** 5 for p in range(3, 9)])
    sns_plot.set_yticks(sns_plot.get_yticks())
    sns_plot.set_yticklabels([f"{p/1000:,.1f}К" for p in sns_plot.get_yticks()])
    sns_plot.set_title('Пропускная способность модели реализации B дерева')
    
    plt.savefig(image_dir / "bench_3.png")

def plot_bench_4(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_4.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    x = ['Чтение 70% Запись 30%', 'Чтение 30% Запись 70%',]
    y = df['total_throughput'][1:]
    sns_plot = sns.barplot(x=x, y=y, ax=ax)
    sns_plot.set(xlabel="Рабочая нагрузка", ylabel="Пропускная способность оп/с")

    sns_plot.set_yticks(sns_plot.get_yticks())
    sns_plot.set_yticklabels([f"{p/1000:,.1f}К" for p in sns_plot.get_yticks()])
    sns_plot.set_title('Пропускная способность модели реализации LSM дерева')
    
    plt.savefig(image_dir / "bench_4.png")

def plot_bench_5(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_5.log")
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    x = ['Модель B дерева. Чтение 70% Запись 30%', 'Модель LSM Дерева. Чтение 30% Запись 70%']
    y = df['total_throughput'][:2]
    sns_plot = sns.barplot(x=x, y=y, ax=ax)
    sns_plot.set(xlabel="Используемый тип хранилища", ylabel="Пропускная способность оп/с")

    sns_plot.set_yticks(sns_plot.get_yticks())
    sns_plot.set_yticklabels([f"{p/1000:,.1f}К" for p in sns_plot.get_yticks()])
    sns_plot.set_title('Пропускная способность при нагрузке 70% - чтение, 30% - запись')
    
    plt.savefig(image_dir / "bench_5.png")

def plot_bench_6_7(log_dir: pathlib.Path, image_dir: pathlib.Path):
    df = pd.read_csv(log_dir / "bench_6.log")
    total_bench_6 = df['total_throughput'].sum()
    df = pd.read_csv(log_dir / "bench_7.log")
    total_bench_7 = df['total_throughput'].sum()
    print(total_bench_6, total_bench_7)
    fig, ax = plt.subplots()
    fig.set_size_inches(11, 7)
    x = ['Реализация SCM Storage', 'Реализация SCM Storage без накладных расходов на persist']
    y = [total_bench_6, total_bench_7]
    sns_plot = sns.barplot(x=x, y=y, ax=ax)
    sns_plot.set(xlabel="Используемый тип хранилища", ylabel="Пропускная способность оп/с")
    # ax.set(yscale="log")
    sns_plot.set_yticks(sns_plot.get_yticks())
    sns_plot.set_yticklabels([f"{p/1000:,.1f}К" for p in sns_plot.get_yticks()])
    sns_plot.set_title('Пропускная способность SCM Storage при нагрузке 70% - чтение, 30% - запись')
    
    plt.savefig(image_dir / "bench_6_7.png")



def main():
    # plot_bench_1_throughput(LOG_DIR, IMAGE_DIR)
    plot_bench_1_mean_latency(LOG_DIR, IMAGE_DIR)
    # plot_bench_2_throughput(LOG_DIR, IMAGE_DIR)
    plot_bench_2_mean_latency(LOG_DIR, IMAGE_DIR)
    # plot_bench_3(LOG_DIR, IMAGE_DIR)
    # plot_bench_4(LOG_DIR, IMAGE_DIR)
    # plot_bench_5(LOG_DIR, IMAGE_DIR)
    # plot_bench_6_7(LOG_DIR, IMAGE_DIR)
    # plot_bench_2(LOG_DIR)


if __name__ == "__main__":
    main()
