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
        st.info(f"💡 **Ajuste automático BBVA:** Se unieron {len(df_restantes)} registro(s) restante(s) (+2) exactos. Ops: {', '.join(ops_ajustadas)}")
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
st.title('📊 Conciliación PAYOUTS día anterior')
st.write('Herramienta para la conciliación de los pagos del día anterior')

archivo_metabase = st.file_uploader('Sube el archivo de payouts del metabase', type=['xlsx'])

if archivo_metabase:
    with st.spinner("Procesando archivo Metabase..."):
        df_metabase_raw = pd.read_excel(archivo_metabase)
        df_metabase = procesar_metabase(df_metabase_raw)
        
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
                        st.toast(f'✅ {archivo.name} procesado correctamente')
                    except Exception as e:
                        st.error(f'Error al procesar {archivo.name}: {str(e)}')
                else:
                    st.warning(f'⚠️ No se encontró un procesador para: {archivo.name}')

        if df_bancos_list:
            df_bancos_final = pd.concat(df_bancos_list, ignore_index=True)
            
            st.subheader("📊 Datos consolidados de los Bancos")
            st.dataframe(df_bancos_final, width='stretch')

            # --- LÓGICA DE CONCILIACIÓN ---
            st.subheader('⚖️ Conciliación Final')
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
                st.warning('⚠️ Se detectaron diferencias en la conciliación. Revisa el detalle a continuación.')
                
                # Agrupación a nivel de Operación para encontrar a los culpables
                ops_banco = df_bancos_final.groupby(['name', 'Operación - Número'])['Monto'].sum().reset_index()
                
                # Para la vista detallada agregamos la hora del Metabase
                ops_metabase = df_metabase.groupby(['name', 'ope_psp']).agg({
                    'monto total': 'sum',
                    'hora': 'first'
                }).reset_index()
                ops_metabase = ops_metabase.rename(columns={'ope_psp': 'Operación - Número'})
                
                # Merge detallado
                df_diferencias_detalle = pd.merge(ops_banco, ops_metabase, on='Operación - Número', how='outer')
                
                # Cálculo de diferencia
                df_diferencias_detalle['Diferencias'] = round(df_diferencias_detalle['monto total'] + df_diferencias_detalle['Monto'], 2)
                
                # Filtrar solo las que no cuadran
                df_diferencias_detalle = df_diferencias_detalle[df_diferencias_detalle['Diferencias'] != 0]

                # --- RENOMBRAMIENTO Y LIMPIEZA VISUAL (Restaurado) ---
                columnas_vista = {
                    'name_x': 'Banco estados de cuenta',
                    'Operación - Número': 'Numero operacion banco',
                    'Monto': 'Monto bancos',
                    'name_y': 'Banco metabase',
                    'monto total': 'Monto metabase'
                }
                df_diferencias_detalle = df_diferencias_detalle.rename(columns=columnas_vista)
                
                # Consolidar nombre final del banco
                df_diferencias_detalle['Banco final'] = df_diferencias_detalle['Banco metabase'].combine_first(df_diferencias_detalle['Banco estados de cuenta'])
                
                # Filtrar solo los bancos que tienen problemas a nivel general
                bancos_con_problemas = df_conciliacion[df_conciliacion['Diferencia'] != 0]['BANCO'].unique()
                df_diferencias_detalle = df_diferencias_detalle[df_diferencias_detalle['Banco final'].isin(bancos_con_problemas)]
                
                # Mostrar solo las columnas relevantes
                columnas_a_mostrar = [
                    'Banco estados de cuenta', 'Numero operacion banco', 'Monto bancos',
                    'Banco metabase', 'Monto metabase', 'hora', 'Diferencias'
                ]
                
                with st.expander('🔍 Detalle de operaciones con diferencias'):
                    st.dataframe(df_diferencias_detalle[columnas_a_mostrar], width='stretch')

                # Marcamos el DF del metabase original con el estado de las diferencias
                operaciones_con_dif = df_diferencias_detalle['Numero operacion banco'].unique()
                mask_diferencias = df_metabase['ope_psp'].isin(operaciones_con_dif)
                df_metabase.loc[mask_diferencias, 'Estado'] = f'Conciliacion_{fecha_reporte} - Diferencias'
            else:
                st.success('🎉 ¡Todos los montos han sido conciliados correctamente!')

            # --- DESCARGA ---
            st.markdown("---")
            excel_data = generar_excel_descarga(df_metabase, df_bancos_final, fecha_reporte)
            
            st.download_button(
                label='⬇️ DESCARGAR CONCILIACIÓN',
                data=excel_data,
                file_name=f'Conciliacion_{fecha_reporte}.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                width='stretch'
            )
