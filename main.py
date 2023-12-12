import os
import stat
import paramiko
import traceback
import pandas as pd
from time import sleep
from multiprocessing import Pool
from datetime import datetime as dt
from colorama import init

init()


path_actual = os.path.dirname(__file__)
os.chdir(path_actual)


def con_sftp(hostname='sftp.mg8.com.br', username: str = None, password: str = None):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    not_conn = True
    cont = 1
    while not_conn:
        try:
            ssh_client.connect(hostname=hostname,
                               username=username,
                               password=password)
            not_conn = False
        except TimeoutError as e:
            if cont > 5:
                raise e
            sleep(5)
            cont += 1
            pass

    return ssh_client.open_sftp()


def verificar_caminho_sftp(sftp, caminho):
    try:
        # Tenta obter informações sobre o arquivo
        sftp.stat(caminho)
        return True  # O arquivo existe
    except FileNotFoundError:
        # O arquivo não existe, tenta criar o caminho
        criar_caminho_sftp(sftp, caminho)
        return False


def verificar_arquivo_sftp(sftp, caminho_arquivo):
    try:
        # Tenta obter informações sobre o arquivo
        sftp.stat(caminho_arquivo)
        return True  # O arquivo existe
    except FileNotFoundError:
        # O arquivo não existe
        return False


def criar_caminho_sftp(sftp, caminho):
    """
    Cria o caminho no servidor SFTP se não existir.
    """
    try:
        sftp.mkdir(caminho)
        print(f'Caminho criado: {caminho}', end='... ')
    except Exception as e:
        print(f'Erro ao criar caminho {caminho}: {e}')


def listar_arquivos_recursivamente(sftp, caminho):
    arquivos = []

    # Lista os itens no diretório
    if '.' in caminho:
        arquivos.append(caminho)
    else:
        itens = sftp.listdir_attr(caminho)

        for item in itens:
            caminho_completo = f"{caminho}/{item.filename}"

            if stat.S_ISDIR(item.st_mode):
                file_extensions = ['.zip', '.rar', '.gz', '.csv']
                valid = True

                for ext in file_extensions:
                    if ext in item.filename:
                        arquivos.append(caminho_completo)
                        valid = False
                        break

                # Se for um diretório, chama recursivamente a função para
                # listar os arquivos dentro do diretório
                if valid:
                    arquivos.extend(listar_arquivos_recursivamente(
                        sftp, caminho_completo))
            else:
                # Se for um arquivo, adiciona o caminho completo à lista
                arquivos.append(caminho_completo)

    return arquivos  # Retorna a lista de arquivos ao final da função


def create_log(log_dir, filename):
    if not os.path.exists('logs/' + log_dir):
        os.makedirs('logs/' + log_dir)

    try:
        with open(f'logs/{log_dir}/{filename}.log', 'a') as log:
            log.write(f'{"="*30}\n{dt.now()} - {traceback.format_exc()} \n\n')

    except Exception:
        with open(f'logs/{log_dir}/{filename}.log', 'w') as log:
            log.write(f'{"="*30}\n{dt.now()} - {traceback.format_exc()} \n\n')


def transferir_arquivo(origem_hostname, origem_username, origem_password,
                       destino_hostname, destino_username, destino_password,
                       arquivo):
    try:
        # Configuração da conexão de origem
        origem_transport = paramiko.Transport((origem_hostname, 22))
        origem_transport.connect(username=origem_username, password=origem_password)
        origem_sftp = paramiko.SFTPClient.from_transport(origem_transport)

        # Configuração da conexão de destino
        destino_transport = paramiko.Transport((destino_hostname, 22))
        destino_transport.connect(username=destino_username, password=destino_password)
        destino_sftp = paramiko.SFTPClient.from_transport(destino_transport)

        destino_sftp.chdir('arquivos')

        with origem_sftp.file(arquivo, 'rb') as arquivo_origem:
            caminho_destino = os.path.dirname(arquivo)
            verificar_caminho_sftp(destino_sftp, caminho_destino)
            if not verificar_arquivo_sftp(destino_sftp, arquivo):
                destino_sftp.putfo(arquivo_origem, arquivo)
                print(f'\033[1;32mArquivo {arquivo} transferido com sucesso!\033[0m')

    except Exception as e:
        print(f'Erro ao transferir o arquivo {arquivo}: {e}')
        create_log(destino_username, filename=dt.today().date())

    finally:
        # Fechar as conexões
        origem_sftp.close()
        destino_sftp.close()


def main():
    df_ = pd.read_excel('sftps.xlsx')

    df = df_[df_['active_sftp'] == True]

    print(df)
    print('='*50 + '\n\n')
    for index, login in df.iterrows():
        origem_hostname = login['origin']
        origem_username = login['login_origin']
        origem_password = login['password_origin']

        # Configurações do segundo servidor SFTP (destino)
        destino_hostname = login['destin']
        destino_username = login['login_destin']
        destino_password = login['password_destin']
        print(f'\n\n\033[33mIniciando conexão SFTP {destino_username}\033[0m')

        tentativas = 10
        for tentativa in range(tentativas):
            # Configurações do primeiro servidor SFTP (origem)

            try:
                origem_sftp = con_sftp(
                    hostname=origem_hostname,
                    username=origem_username,
                    password=origem_password
                )

                destino_sftp = con_sftp(
                    hostname=destino_hostname,
                    username=destino_username,
                    password=destino_password
                )

            except Exception:
                create_log(destino_username, filename=dt.today().date())
                raise

            # Lista os arquivos de origem
            print("Lendo a pasta origem...")
            arquivos_origem = []
            dirs = origem_sftp.listdir()
            try:
                for x in dirs:
                    print(f"Lendo a pasta {x}", end='... ')
                    arquivos_origem += listar_arquivos_recursivamente(
                        origem_sftp, x)

                    print("\033[32mPasta lida com sucesso!\033[0m")
            except Exception:
                create_log(destino_username, filename=dt.today().date())

            # Lista os arquivos de destino
            print("\n\n" + "="*60)
            print("Lendo a pasta destino...")
            destino_sftp.chdir('arquivos')
            arquivos_destino = []
            dirs = destino_sftp.listdir()
            try:
                for x in dirs:
                    print(f"Lendo a pasta {x}", end='... ')
                    arquivos_destino += listar_arquivos_recursivamente(destino_sftp, x)
                    print("\033[32mPasta lida com sucesso!\033[0m")
            except Exception:
                create_log(destino_username, filename=dt.today().date())

            arquivos_unicos = [x for x in arquivos_origem if x not in arquivos_destino]

            if arquivos_unicos == []:
                origem_sftp.close()
                destino_sftp.close()
                break

            print(f"Lendo um total de {len(arquivos_unicos)} arquivos")
            with Pool(processes=4) as pool:
                pool.starmap(transferir_arquivo,
                             [(origem_hostname, origem_username, origem_password,
                              destino_hostname, destino_username, destino_password,
                              arquivo) for arquivo in arquivos_unicos[::1]])

            # Fechar as conexões
            origem_sftp.close()
            destino_sftp.close()
            if tentativa == tentativas - 1:
                print('O número máximo de tentativas foi alcançado, tente novamente mais tarde.')
            else:
                print(f'\n Tentativa {tentativa+1} de {tentativas} - Tentando novamente arquivos com erro...')
            sleep(20)


if __name__ == '__main__':
    main()
