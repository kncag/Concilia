import pandas as pd
import streamlit as st
import io

# =========================================
# CONSTANTES
# =========================================
BANCO_BCP = '(BCP) - Banco de Crédito del Perú'
BANCO_IBK = '(Interbank) - Banco International del Perú'
BANCO_BBVA = '(BBVA) - BBVA Continental'
BANCO_OTROS = 'Otros bancos'

BANCOS_PRINCIPALES = [BANCO_BCP, BANCO_IBK, BANCO_BBVA]

# =========================================
# FUNCIONES AUXILIARES
# =========================================
def limpiar_numero_operacion(series: pd.Series) -> pd.Series:
    """
    Limpia la columna de números de operación evitando errores de casting estricto.
    Convierte a numérico, redondea para evitar decimales de Excel y pasa a string sin ceros iniciales.
    """
    return pd.to_numeric(series, errors='coerce').apply(
        lambda x: str(int(round(x))) if pd.notna(x) else None
    )

# =========================================
# PROCESAMIENTO METABASE
# =========================================
def procesar_metabase(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y formatea el DataFrame base del Metabase.
    """
    df = df.copy()
    
    # Limpieza de OP_PSP
    df['ope_psp'] = limpiar_numero_operacion(df['ope_psp'])

    # Fechas
    df['fecha_proceso'] = pd.to_datetime(df['fecha pagado / rechazado']).dt.normalize()
    df['hora'] = pd.to_datetime(df['fecha proceso']).dt.hour
    df['date'] = pd.to_datetime(df['fecha proceso']).dt.date

    # Filtros base
    mask_validos = (
        (df['estado'] == 'Pagado') & 
        (df['moneda'] == 'PEN') & 
        (df['name'] != '(Scotiabank)- Scotiabank') &
        (~df['name'].astype(str).str.contains('Yape', case=False, na=False))
    )
    df_filtrado = df[mask_validos].copy()

    # Normalizar nombres de bancos
    df_filtrado['name'] = df_filtrado['name'].apply(
        lambda x: x if x in BANCOS_PRINCIPALES else BANCO_OTROS
    )
    
    return df_filtrado

# =========================================
# PROCESAMIENTO ESTADOS DE CUENTA
# =========================================
def procesar_bcp(archivo) -> pd.DataFrame:
    df = pd.read_excel(archivo, skiprows=4)
    df['Operación - Número'] = df['Operación - Número'].astype(str)
    df = df[df['Referencia2'].str.contains('PAYOUT', case=False, na=False)].copy()

    df['Hora'] = pd.to_datetime(df['Operación - Hora'], format='%H:%M:%S', errors='coerce').dt.hour

    suma_monto_por_hora = df.groupby('Hora')['Monto'].sum()
    
    df_consolidado = df[df['Monto'] < 0].drop_duplicates(subset=['Hora']).copy()
    df_consolidado['Monto'] = df_consolidado['Hora'].map(suma_monto_por_hora)

    cols_mantener = ['Operación - Número', 'Referencia2', 'Monto']
    df_consolidado = df_consolidado[[col for col in cols_mantener if col in df_consolidado.columns]]
    
    df_consolidado['name'] = BANCO_BCP
    return df_consolidado

def procesar_interbank(archivo) -> pd.DataFrame:
    df = pd.read_excel(archivo, skiprows=13).drop(columns=['Unnamed: 0'], errors='ignore')
    
    renombres = {
        'Fecha de Proc.': 'Fecha', 'Cargos':'Monto', 
        'Detalle': 'Referencia2', 'Cod. de Operación': 'Operación - Número'
    }
    df = df.rename(columns=renombres)

    mask_payouts = df['Referencia2'].str.contains(r'\b(?:PA(?:Y(?:OU(?:T)?)?|YO|YOU)?|PAYOUTS?(?:\s+VARI)?|VARI)\b', case=False, na=False)
    df = df[mask_payouts].copy()

    df['Operación - Número'] = limpiar_numero_operacion(df['Operación - Número'])
    df['name'] = BANCO_IBK
    
    cols_drop = ['Fecha de Op.', 'Movimiento', 'Canal', 'Cod. de Ubicación', 'Abonos', 'Saldo contable']
    return df.drop(columns=[c for c in cols_drop if c in df.columns])

def procesar_bbva_otros(archivo, df_metabase: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_excel(archivo, skiprows=10)
    
    renombres = {
        'F. Operación': 'Fecha', 'Concepto': 'Referencia2', 
        'Importe': 'Monto', 'Nº. Doc.':'Operación - Número'
    }
    df = df.rename(columns=renombres)
    df['Operación - Número'] = df['Operación - Número'].astype(str).str.strip()
    
    # 1. Identificar operaciones del BBVA principal
    valores_metabase = set(
        df_metabase[df_metabase['name'] == BANCO_BBVA]['ope_psp'].dropna().astype(str).str.strip()
    )
    
    mask_bbva = [any(valor in str(x) for valor in valores_metabase) for x in df['Operación - Número']]
    df_bbva_causantes = df[mask_bbva].copy()
    df_bbva_causantes['Operación - Número'] = limpiar_numero_operacion(df_bbva_causantes['Operación - Número'])
    df_bbva_causantes['name'] = BANCO_BBVA

    # Lógica estricta de ajuste de sobrantes (+2)
    df_bbva_causantes = ajustar_diferencias_bbva(df_bbva_causantes, df, df_metabase)

    # 2. Identificar operaciones de "Otros bancos" (BXI)
    mask_otros = df['Referencia2'].astype(str).str.contains('BXI', case=False, na=False)
    df_otros = df[mask_otros].copy()
    
    df_otros['Operación - Número'] = df_otros['Referencia2'].str.extract(r'(\d{5,})$')[0]
    df_otros['Operación - Número'] = limpiar_numero_operacion(df_otros['Operación - Número'])
    df_otros['name'] = BANCO_OTROS

    df_filtrado = pd.concat([df_bbva_causantes, df_otros], ignore_index=True)
    return df_filtrado.drop(columns=['F. Valor', 'Código', 'Oficina', 'Op_Temp_Int'], errors='ignore')

def ajustar_diferencias_bbva(df_causantes: pd.DataFrame, df_original: pd.DataFrame, df_metabase: pd.DataFrame) -> pd.DataFrame:
    """
    Busca operaciones complementarias (+2) en BBVA solo si existe una diferencia positiva exacta.
    """
    metabase_bbva = df_metabase[df_metabase['name'] == BANCO_BBVA].copy()
    metabase_bbva['ope_psp'] = metabase_bbva['ope_psp'].astype(str).str.strip()
    montos_esperados = metabase_bbva.groupby('ope_psp')['monto total'].sum()
    montos_actuales = df_causantes.groupby('Operación - Número')['Monto'].sum()

    restantes = []
    ops_ajustadas = set()

    for op in df_causantes['Operación - Número'].dropna().unique():
        if op in montos_esperados.index:
            diferencia = round(montos_esperados[op] + montos_actuales.get(op, 0), 2)
            
            if diferencia != 0:
                monto_buscado = round(-diferencia, 2)
                
                if monto_buscado > 0:
                    try:
                        op_int = int(op)
                        op_target_int = op_int + 2
                        
                        mask_exacta = (
                            (pd.to_numeric(df_original['Operación - Número'], errors='coerce') == op_target_int) &
                            (df_original['Monto'].round(2) == monto_buscado)
                        )
                        
                        match_df = df_original[mask_exacta].copy()
                        
                        if not match_df.empty:
                            match_df['Operación - Número'] = str(op_int)  
                            match_df['name'] = BANCO_BBVA
                            restantes.append(match_df)
                            ops_ajustadas.add(str(op_int))
                    except ValueError:
                        pass 
    
    if restantes:
        df_restantes = pd.concat(restantes, ignore_index=True)
        st.info(f"**Ajuste automático BBVA:** Se unieron {len(df_restantes)} registro(s) restante(s) (+2) exactos. Ops: {', '.join(ops_ajustadas)}")
        return pd.concat([df_causantes, df_restantes], ignore_index=True)
    
    return df_causantes.copy()

def generar_excel_descarga(df_metabase: pd.DataFrame, df_bancos: pd.DataFrame, fecha: str) -> bytes:
    """Genera el buffer en memoria del archivo Excel para descargar."""
    df_metabase['Estado'] = f'Conciliacion_{fecha}'
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        df_metabase.to_excel(writer, sheet_name='Payouts_Metabase', index=False)
        df_bancos.to_excel(writer, sheet_name='Operaciones Bancos', index=False)
    return excel_buffer.getvalue()


# =========================================
# INTERFAZ PRINCIPAL STREAMLIT
# =========================================
st.set_page_config(page_title="Conciliación Payouts", layout="centered")
st.title('Conciliación PAYOUTS día anterior')
st.write('Herramienta para la conciliación de los pagos del día anterior')

archivo_metabase = st.file_uploader('Sube el archivo de payouts del metabase', type=['xlsx'])

if archivo_metabase:
    # --- IMPLEMENTACIÓN DE SESSION STATE ---
    # Esto asegura que al modificar Metabase en la UI, no se vuelva a cargar el original
    if 'uploaded_file_name' not in st.session_state or st.session_state.uploaded_file_name != archivo_metabase.name:
        with st.spinner("Procesando archivo Metabase..."):
            df_metabase_raw = pd.read_excel(archivo_metabase)
            st.session_state.df_metabase = procesar_metabase(df_metabase_raw)
            st.session_state.uploaded_file_name = archivo_metabase.name

    # Trabajamos con el dataframe almacenado en memoria (que puede tener correcciones)
    df_metabase = st.session_state.df_metabase.copy()
        
    fecha_reporte = df_metabase['fecha_proceso'].dropna().unique()[0].strftime("%Y%m%d")

    # Resumen Metabase
    st.subheader("Datos consolidados Metabase")
    pivot_metabase = df_metabase.groupby(['fecha_proceso','name'])['monto total'].sum().reset_index()
    st.dataframe(pivot_metabase, width='stretch')

    # Diccionario de procesadores
    procesadores_banco = {
        'bcp': procesar_bcp,
        'ibk': procesar_interbank,
        'bbva': lambda arc: procesar_bbva_otros(arc, df_metabase) # Pasamos df_metabase al BBVA
    }

    archivos_bancos = st.file_uploader('Subir estados de cuenta bancarios', type=['xlsx', 'xls'], accept_multiple_files=True)
    
    if archivos_bancos:
        df_bancos_list = []

        with st.spinner("Procesando estados de cuenta..."):
            for archivo in archivos_bancos:
                nombre_archivo = archivo.name.lower()
                procesador = next((func for clave, func in procesadores_banco.items() if clave in nombre_archivo), None)

                if procesador:
                    try:
                        df_procesado = procesador(archivo)
                        df_bancos_list.append(df_procesado)
                    except Exception as e:
                        st.error(f'Error al procesar {archivo.name}: {str(e)}')
                else:
                    st.warning(f'No se encontró un procesador para: {archivo.name}')

        if df_bancos_list:
            df_bancos_final = pd.concat(df_bancos_list, ignore_index=True)
            
            st.subheader("Datos consolidados de los Bancos")
            st.dataframe(df_bancos_final, width='stretch')

            # --- LÓGICA DE CONCILIACIÓN ---
            st.subheader('Conciliación Final')
            st.write('Comparación entre los montos de los bancos y el metabase del core de Kashio.')

            # Agrupaciones
            montos_bancos_agrupados = df_bancos_final.groupby('name')['Monto'].sum().reset_index()
            montos_bancos_agrupados['Monto'] = montos_bancos_agrupados['Monto'].abs()
            
            montos_metabase_agrupados = df_metabase.groupby(['name', 'fecha_proceso'])['monto total'].sum().reset_index()

            # Merge final
            df_conciliacion = pd.merge(montos_metabase_agrupados, montos_bancos_agrupados, on='name', how='outer')
            df_conciliacion['Diferencia'] = round(df_conciliacion['monto total'] - df_conciliacion['Monto'], 2)
            df_conciliacion['Estado'] = df_conciliacion['Diferencia'].apply(lambda x: 'Conciliado' if x == 0 else 'Diferencias')
            
            df_conciliacion['fecha_proceso'] = df_conciliacion['fecha_proceso'].ffill().bfill()

            # Formateo visual
            renombres_conciliacion = {
                'fecha_proceso': 'FechaProceso', 'name':'BANCO', 
                'monto total':'Monto Kashio', 'Monto':'Monto Banco'
            }
            df_conciliacion_visual = df_conciliacion.rename(columns=renombres_conciliacion)
            st.dataframe(df_conciliacion_visual, width='stretch')

            # --- ANÁLISIS DE DIFERENCIAS ---
            if 'Diferencias' in df_conciliacion['Estado'].values:
                st.warning('Se detectaron diferencias en la conciliación. Revisa el detalle a continuación.')
                
                # 1. Agrupación matemática TOTAL a nivel de Operación para calcular el descuadre real
                ops_banco_total = df_bancos_final.groupby(['name', 'Operación - Número'])['Monto'].sum().reset_index()
                # Duplicamos las columnas para poder mostrarlas tras el merge outer
                ops_banco_total['Banco estados de cuenta'] = ops_banco_total['name']
                ops_banco_total['Numero operacion banco'] = ops_banco_total['Operación - Número']

                ops_metabase_total = df_metabase.groupby(['name', 'ope_psp'])['monto total'].sum().reset_index()
                ops_metabase_total = ops_metabase_total.rename(columns={'ope_psp': 'Operación - Número'})
                # Duplicamos las columnas para poder mostrarlas tras el merge outer
                ops_metabase_total['Banco metabase'] = ops_metabase_total['name']
                ops_metabase_total['Numero operacion metabase'] = ops_metabase_total['Operación - Número']
                
                # Merge matemático para encontrar los descuadres netos
                df_dif_totales = pd.merge(ops_banco_total, ops_metabase_total, on=['name', 'Operación - Número'], how='outer')
                
                # Usamos fillna(0) en el cálculo para evitar que los NaN anulen las diferencias de ops que solo existen en un lado
                df_dif_totales['Diferencia_Total'] = round(df_dif_totales['monto total'].fillna(0) + df_dif_totales['Monto'].fillna(0), 2)
                
                # Nos quedamos solo con las operaciones que NO cuadran
                df_dif_totales = df_dif_totales[df_dif_totales['Diferencia_Total'] != 0]

                # 2. Extraer desglose por HORA desde el Metabase
                ops_metabase_hora = df_metabase.groupby(['name', 'ope_psp', 'hora'])['monto total'].sum().reset_index()
                ops_metabase_hora = ops_metabase_hora.rename(columns={
                    'ope_psp': 'Operación - Número',
                    'monto total': 'Monto metabase (Parcial por Hora)'
                })

                # 3. Cruzar el resumen matemático con el desglose por hora
                df_diferencias_detalle = pd.merge(
                    df_dif_totales, 
                    ops_metabase_hora, 
                    on=['name', 'Operación - Número'], 
                    how='left'
                )

                # --- RENOMBRAMIENTO Y LIMPIEZA VISUAL PARA LA VISTA DESGLOSADA ---
                columnas_vista = {
                    'Monto': 'Monto bancos (Total)',
                    'monto total': 'Monto metabase (Total)',
                    'Diferencia_Total': 'Diferencias',
                    'hora': 'Hora metabase'
                }
                df_diferencias_detalle = df_diferencias_detalle.rename(columns=columnas_vista)
                
                # Filtrar solo los bancos que tienen problemas a nivel general
                bancos_con_problemas = df_conciliacion[df_conciliacion['Diferencia'] != 0]['name'].unique()
                df_diferencias_detalle = df_diferencias_detalle[df_diferencias_detalle['name'].isin(bancos_con_problemas)]
                
                # Ordenar las columnas para mejor legibilidad, incluyendo explícitamente ambas fuentes
                columnas_a_mostrar = [
                    'Banco estados de cuenta', 'Numero operacion banco', 'Monto bancos (Total)', 
                    'Banco metabase', 'Numero operacion metabase', 'Monto metabase (Total)', 
                    'Diferencias', 'Hora metabase', 'Monto metabase (Parcial por Hora)'
                ]
                
                with st.expander('Detalle de operaciones con diferencias (Desglose por hora)'):
                    st.dataframe(df_diferencias_detalle[columnas_a_mostrar], width='stretch')

                    # --- NUEVA LÓGICA: CORRECCIÓN INTERACTIVA DE METABASE ---
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.toggle("Diferencia por N op (Corrector)"):
                        st.write("1. Marca las casillas de los registros de Metabase que deseas modificar.\n2. Escribe el nuevo número de operación y pulsa Aplicar.")
                        
                        df_to_edit = df_diferencias_detalle[columnas_a_mostrar].copy()
                        df_to_edit.insert(0, 'Seleccionar', False)
                        
                        edited_df = st.data_editor(
                            df_to_edit, 
                            width='stretch', 
                            hide_index=True,
                            column_config={"Seleccionar": st.column_config.CheckboxColumn(required=True)}
                        )
                        
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            nuevo_n_op = st.text_input("Nuevo Número de Operación:", placeholder="Ej: 57299")
                        with col2:
                            st.markdown("<br>", unsafe_allow_html=True)
                            if st.button("Aplicar Cambios a Metabase", use_container_width=True):
                                filas_seleccionadas = edited_df[edited_df['Seleccionar']]
                                
                                if not filas_seleccionadas.empty and nuevo_n_op:
                                    cambios_realizados = 0
                                    
                                    # Formatear el nuevo número evitando decimales no deseados
                                    op_limpio = str(int(round(float(nuevo_n_op)))) if nuevo_n_op.replace('.', '', 1).isdigit() else str(nuevo_n_op).strip()

                                    for _, row in filas_seleccionadas.iterrows():
                                        # Si la fila no existe en Metabase, no podemos actualizarla
                                        if pd.isna(row['Banco metabase']):
                                            continue 
                                        
                                        op_actual = str(row['Numero operacion metabase']).strip()
                                        
                                        # Encontrar la fila exacta en el df almacenado en session_state
                                        mask_meta = (
                                            (st.session_state.df_metabase['name'] == row['Banco metabase']) &
                                            (st.session_state.df_metabase['ope_psp'].astype(str) == op_actual) &
                                            (st.session_state.df_metabase['hora'] == row['Hora metabase'])
                                        )
                                        
                                        if mask_meta.any():
                                            st.session_state.df_metabase.loc[mask_meta, 'ope_psp'] = op_limpio
                                            cambios_realizados += 1
                                            
                                    if cambios_realizados > 0:
                                        st.success(f"Se actualizaron {cambios_realizados} registro(s). Recalculando...")
                                        st.rerun() # Reinicia la app para procesar con los datos modificados
                                    else:
                                        st.warning("No se encontraron registros válidos en Metabase para aplicar el cambio.")
                                        
                                elif not nuevo_n_op:
                                    st.warning("Por favor, ingresa el nuevo número de operación.")
                                else:
                                    st.warning("Por favor, selecciona al menos una fila marcando la casilla.")

                # Marcamos el DF del metabase original con el estado de las diferencias (sin modificar la session cruda)
                operaciones_con_dif = df_diferencias_detalle['Operación - Número'].dropna().unique()
                mask_diferencias = df_metabase['ope_psp'].isin(operaciones_con_dif)
                df_metabase.loc[mask_diferencias, 'Estado'] = f'Conciliacion_{fecha_reporte} - Diferencias'
            else:
                st.success('Todos los montos han sido conciliados correctamente.')

            # --- DESCARGA ---
            excel_data = generar_excel_descarga(df_metabase, df_bancos_final, fecha_reporte)
            
            st.download_button(
                label='DESCARGAR CONCILIACIÓN',
                data=excel_data,
                file_name=f'Conciliacion_{fecha_reporte}.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                width='stretch'
            )
