import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from time import sleep


def get_existing_files(directory: str) -> set:
    """Obtiene el set de nombres de archivos existentes en el directorio"""
    try:
        if not os.path.exists(directory):
            print(f"El directorio {directory} no existe. Creándolo...")
            os.makedirs(directory)
            return set()
        
        return {f for f in os.listdir(directory) if f.endswith('.XML')}
    except Exception as e:
        print(f"Error al leer directorio: {str(e)}")
        return set()


if __name__ == "__main__":
    # Configurar Firefox en modo headless
    firefox_options = Options()
    # firefox_options.add_argument("--headless")

    # Obtener archivos existentes
    xml_directory = "xmlFiles_commodity_tickets"
    existing_files = get_existing_files(xml_directory)
    print(f"Encontrados {len(existing_files)} archivos existentes en {xml_directory}")

    driver = webdriver.Firefox(options=firefox_options)
    print("Iniciando navegador en modo headless...")

    driver.get("https://ftp.culturatech.com/")
    print("Accediendo a la página de login...")

    username_input = driver.find_element(By.XPATH, '//*[@id="username"]')
    password_input = driver.find_element(By.XPATH, '//*[@id="password"]')

    username_input.send_keys("arturo.bravo")
    password_input.send_keys("Elia050593!!!")
    print("Credenciales ingresadas...")

    login_button = driver.find_element(By.XPATH, '//*[@id="login-button"]')
    login_button.click()
    print("Login exitoso...")

    wait = WebDriverWait(driver, 10)

    driver.get(
        'https://ftp.culturatech.com/file/d/Customer%20Data/HEAR002/DAPMonitorFiles/')
    print("Navegando a la página de archivos...")

    filter_input = wait.until(EC.presence_of_element_located(
        (By.XPATH, '/html/body/div[2]/div[2]/form/div[2]/div[1]/div/div[1]/div[1]/div/input')
    ))

    filter_input.send_keys("TICKETS")

    sleep(70)

    # next_button =  wait.until(EC.presence_of_element_located(
    #     (By.XPATH, '//*[@id="fileList_next"]/a')
    # ))

    # next_button.click()

    tbody = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[2]/div[2]/form/div[2]/table/tbody')
        )
    )

    rows = tbody.find_elements(By.TAG_NAME, 'tr')
    print(f"Encontrados {len(rows)} elementos en la tabla...")
    print("Expandiendo resultados...")
    for row in rows:
        try:
            expand_link = row.find_element(
                By.CSS_SELECTOR, 'a[title="Expand"]')
            expand_link.click()
        except Exception as e:
            continue
    
    print("Resultados expandidos... Verificando archivos nuevos...")
    download_btns = tbody.find_elements(
        By.CSS_SELECTOR, 'a[name="dl-file-btn"]')
    
    new_files = 0
    skipped_files = 0
    
    for idx, download_btn in enumerate(download_btns, 1):
        try:
            # Obtener el nombre del archivo desde el elemento padre o el href
            file_name = download_btn.get_attribute('href').split('/')[-1].split('?')[0]
            
            # Verificar si el archivo ya existe
            if file_name in existing_files:
                print(f"Archivo {idx}/{len(download_btns)}: {file_name} - Ya existe, omitiendo...")
                skipped_files += 1
                continue
                
            print(f"Descargando archivo {idx}/{len(download_btns)}: {file_name}")
            download_btn.click()
            new_files += 1
            
        except Exception as e:
            print(f"Error al procesar archivo {idx}: {str(e)}")

    print("\nResumen del proceso:")
    print(f"Total de archivos encontrados: {len(download_btns)}")
    print(f"Archivos nuevos descargados: {new_files}")
    print(f"Archivos omitidos (ya existentes): {skipped_files}")
    print("Proceso completado!")

    sleep(60)
    
    driver.quit()