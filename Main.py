import pandas as pd
import streamlit as st
from datetime import datetime
import io

#=========================================
# Primera parte. Subida archivo METABASE
#=========================================

st.title('Conciliación PAYOUTS día anterior')
st.write('Herramienta para la conciliación de los pagos del día anterior')

payouts_metabase = st.file_uploader('Sube el archivo de payouts del metabase', type=['xlsx'])

if payouts_metabase is not None:
    # 1. Carga y conversiones base
    payouts_metabase_df = pd.read_excel(payouts_metabase)
    
    payouts_metabase_df['ope_psp'] = (
        pd.to_numeric(payouts_metabase_df['ope_psp'], errors='coerce')
        .astype('Int64')
        .astype(str)
    )

    # Convertimos fecha y normalizamos (pone la hora en 00:00:00 sin cambiar el tipo de dato)
    payouts_metabase_df['fecha_proceso'] = pd.to_datetime(payouts_metabase_df['fecha pagado / rechazado']).dt.normalize()
    payouts_metabase_df['hora'] = pd.to_datetime(payouts_metabase_df['fecha proceso']).dt.hour
    payouts_metabase_df['date'] = pd.to_datetime(payouts_metabase_df['fecha proceso']).dt.date

    fecha = payouts_metabase_df['fecha_proceso'].dropna().unique()[0].strftime("%Y%m%d")

    # 2. Filtros combinados (Mejora de rendimiento)
    mask = (
        (payouts_metabase_df['estado'] == 'Pagado') & 
        (payouts_metabase_df['moneda'] == 'PEN') & 
        (payouts_metabase_df['name'] != '(Scotiabank)- Scotiabank')
    )
    payouts_metabase_df = payouts_metabase_df[mask].copy()

    # Tablas Pivot
    pivot_payouts = payouts_metabase_df.groupby(['fecha_proceso','name'])['monto total'].sum().reset_index()
    
    group_hour = payouts_metabase_df.groupby(['name', 'ope_psp']).agg({'monto total':'sum'}).reset_index()
    group_hour = group_hour.rename(columns={'ope_psp':'Operación - Número'})

    st.dataframe(pivot_payouts, use_container_width=True)

    #=========================================
    # FUNCIONES DE BANCOS
    #=========================================  
    
    def procesar_bcp(archivo):
        bcp_eecc = pd.read_excel(archivo, skiprows=4)
        bcp_eecc['Operación - Número'] = bcp_eecc['Operación - Número'].astype(str)
        bcp_eecc = bcp_eecc[bcp_eecc['Referencia2'].str.contains('PAYOUT', case=False, na=False)].copy()

        bcp_eecc['Hora'] = pd.to_datetime(bcp_eecc['Operación - Hora'], format='%H:%M:%S', errors='coerce').dt.hour

        # Optimizacion: Usamos map en lugar de merge para cruzar la suma
        suma_monto_por_hora = bcp_eecc.groupby('Hora')['Monto'].sum()
        
        bcp_consolidado = bcp_eecc[bcp_eecc['Monto'] < 0].drop_duplicates(subset=['Hora']).copy()
        bcp_consolidado['Monto'] = bcp_consolidado['Hora'].map(suma_monto_por_hora)

        columnas_mantener = ['Operación - Número', 'Referencia2', 'Monto']
        bcp_consolidado = bcp_consolidado[[col for col in columnas_mantener if col in bcp_consolidado.columns]]
        
        bcp_consolidado['name'] = '(BCP) - Banco de Crédito del Perú'
        return bcp_consolidado
    
    def procesar_interbank(archivo):
        ibk_eecc = pd.read_excel(archivo, skiprows=13).drop(columns=['Unnamed: 0'], errors='ignore')
        
        columns_name = {
            'Fecha de Proc.': 'Fecha', 'Cargos':'Monto', 
            'Detalle': 'Referencia2', 'Cod. de Operación': 'Operación - Número'
        }
        ibk_eecc = ibk_eecc.rename(columns=columns_name)

        ibk_eecc = ibk_eecc[ibk_eecc['Referencia2'].str.contains(r'\b(?:PA(?:Y(?:OU(?:T)?)?|YO|YOU)?|PAYOUTS?(?:\s+VARI)?|VARI)\b', case=False, na=False)].copy()

        # Evita errores si hay valores nulos antes de pasar a string
        ibk_eecc['Operación - Número'] = pd.to_numeric(ibk_eecc['Operación - Número'], errors='coerce').astype('Int64').astype(str)
        ibk_eecc['name'] = '(Interbank) - Banco International del Perú'
        
        cols_drop = ['Fecha de Op.', 'Movimiento', 'Canal', 'Cod. de Ubicación', 'Abonos', 'Saldo contable']
        return ibk_eecc.drop(columns=[c for c in cols_drop if c in ibk_eecc.columns])

    def procesar_bbva_otros(archivo):
        bancos_bbva = pd.read_excel(archivo, skiprows=10)
        
        columns_name = {
            'F. Operación': 'Fecha', 'Concepto': 'Referencia2', 
            'Importe': 'Monto', 'Nº. Doc.':'Operación - Número'
        }
        bancos_bbva = bancos_bbva.rename(columns=columns_name)
        
        # Usamos set para búsquedas más rápidas
        valores_metabase = set(
            payouts_metabase_df[payouts_metabase_df['name'] == '(BBVA) - BBVA Continental']['ope_psp']
            .dropna().astype(str).str.strip()
        )
        
        bancos_bbva['Operación - Número'] = bancos_bbva['Operación - Número'].astype(str).str.strip()
        
        # Comprensión de listas (más rápido que lambda apply)
        mask_bbva = [any(valor in str(x) for valor in valores_metabase) for x in bancos_bbva['Operación - Número']]
        df_bbva_causantes = bancos_bbva[mask_bbva].copy()
        
        df_bbva_causantes['Operación - Número'] = (
            pd.to_numeric(df_bbva_causantes['Operación - Número'], errors='coerce')
            .astype('Int64').astype(str)
        )
        df_bbva_causantes['name'] = '(BBVA) - BBVA Continental'

        # ======== LÓGICA RESTANTES (+2) EN BBVA ESTRICTA ========
        # 1. Calcular montos esperados del metabase para BBVA
        metabase_bbva = payouts_metabase_df[payouts_metabase_df['name'] == '(BBVA) - BBVA Continental'].copy()
        metabase_bbva['ope_psp'] = metabase_bbva['ope_psp'].astype(str).str.strip()
        expected_amounts = metabase_bbva.groupby('ope_psp')['monto total'].sum()

        # 2. Calcular montos encontrados actualmente en el estado de cuenta
        current_amounts = df_bbva_causantes.groupby('Operación - Número')['Monto'].sum()

        restantes_encontrados = []
        ops_ajustadas = []

        # 3. Detectar ops con diferencias e intentar cuadrar de forma precisa
        for op in df_bbva_causantes['Operación - Número'].unique():
            if op in expected_amounts.index:
                # Calcula la diferencia actual (Monto Kashio + Monto Banco)
                diferencia = round(expected_amounts[op] + current_amounts.get(op, 0), 2)
                
                if diferencia != 0:
                    # El monto requerido del banco para equilibrar a cero es la diferencia invertida
                    monto_buscado = round(-diferencia, 2)
                    
                    # REGLA: Solo buscamos si el monto requerido es un valor POSITIVO
                    if monto_buscado > 0:
                        try:
                            op_int = int(op)
                            op_target_int = op_int + 2
                            
                            # Filtro muy estricto: Coincidencia numérica de Op + 2 Y coincidencia exacta de Monto
                            mask_exacta = (
                                (pd.to_numeric(bancos_bbva['Operación - Número'], errors='coerce') == op_target_int) &
                                (bancos_bbva['Monto'].round(2) == monto_buscado)
                            )
                            
                            match_df = bancos_bbva[mask_exacta].copy()
                            
                            # Si encuentra algo, lo acepta como restante válido
                            if not match_df.empty:
                                match_df['Operación - Número'] = str(op_int)  # Se unifica el ID
                                match_df['name'] = '(BBVA) - BBVA Continental'
                                restantes_encontrados.append(match_df)
                                ops_ajustadas.append(str(op_int))
                        except ValueError:
                            pass # Manejo por si el número de op tiene letras
        
        # 4. Integrar los registros válidos o dejar las cosas como están
        if restantes_encontrados:
            df_restantes_validos = pd.concat(restantes_encontrados, ignore_index=True)
            df_bbva = pd.concat([df_bbva_causantes, df_restantes_validos], ignore_index=True)
            
            # --- Notificación al usuario con validación estricta ---
            st.info(f"💡 **Ajuste automático BBVA:** Se detectaron {len(df_restantes_validos)} registro(s) restante(s) (+2) con montos positivos que cuadran **exactamente** la diferencia. Operación(es) consolidada(s): {', '.join(set(ops_ajustadas))}")
        else:
            df_bbva = df_bbva_causantes.copy()
        # ==========================================================

        # ======== SECCIÓN OTROS BANCOS (BXI) ========
        df_otros = bancos_bbva[bancos_bbva['Referencia2'].astype(str).str.contains('BXI', case=False, na=False)].copy()
        
        # Extraemos el código para cruzar con metabase
        df_otros['Operación - Número'] = df_otros['Referencia2'].str.extract(r'(\d{5,})$')[0]
        df_otros['name'] = 'Otros bancos'

        bancos_bbva_filtrado = pd.concat([df_bbva, df_otros], ignore_index=True)
        return bancos_bbva_filtrado.drop(columns=['F. Valor', 'Código', 'Oficina', 'Op_Temp_Int'], errors='ignore')

    #=========================================
    # LECTURA DE ESTADOS DE CUENTA
    #=========================================  

    procesadores_banck = {
        'bcp': procesar_bcp,
        'ibk': procesar_interbank,
        'bbva': procesar_bbva_otros
    }

    estado_cuenta = st.file_uploader('Subir estados de cuenta', type=['xlsx', 'xls'], accept_multiple_files=True)
    
    df_consolidados = []

    if estado_cuenta:
        for archivo in estado_cuenta:
            nombre_archivo = archivo.name.lower()
            procesador = next((funcion for clave, funcion in procesadores_banck.items() if clave in nombre_archivo), None)

            if procesador:
                try:
                    df = procesador(archivo)
                    df_consolidados.append(df)
                    st.success(f'Archivo procesado: {archivo.name}')
                except Exception as e:
                    st.error(f'Error al procesar {archivo.name}: {e}')
            else:
                st.warning(f'No se encontró una función para procesar: {archivo.name}')

    if df_consolidados:
        df_final = pd.concat(df_consolidados, ignore_index=True)
        st.subheader("📊 Datos consolidados de todos los bancos")
        
        df_final_group = df_final.groupby(['name', 'Operación - Número']).agg({'Monto':'sum'}).reset_index()
        
        # Corrección: Extraer la hora de forma segura
        group_hour = payouts_metabase_df.groupby(['name', 'ope_psp']).agg({
            'monto total':'sum', 
            'hora': 'first' # 'first' es más seguro y rápido que lambda x: x.unique()[0]
        }).reset_index() 
        group_hour = group_hour.rename(columns={'ope_psp':'Operación - Número'})
        
        st.dataframe(df_final)

        merge_op = pd.merge(df_final_group, group_hour, on='Operación - Número', how='outer')
        merge_op['Diferencias'] = round((merge_op['monto total'] + merge_op['Monto']), 2)
        merge_op = merge_op[merge_op['Diferencias'] != 0]

        bancos_montos = df_final.groupby('name')['Monto'].sum().reset_index() 
        bancos_montos['Monto'] = bancos_montos['Monto'].abs()

        #=========================================
        # CONCILIACIÓN FINAL
        #========================================= 

        st.subheader('Conciliación de los montos de todos los bancos')
        st.write('''En esta sección podremos encontrar si hay diferencias entre los montos de los 
                    bancos de los estados de cuenta y el metabase del core de Kashio...''')
        
        conciliacion_payouts = pd.merge(pivot_payouts, bancos_montos, on='name', how='outer')
        conciliacion_payouts['Diferencia'] = round(conciliacion_payouts['monto total'] - conciliacion_payouts['Monto'], 2)
        conciliacion_payouts['Estado'] = conciliacion_payouts['Diferencia'].apply(lambda x: 'Conciliado' if x == 0 else 'Diferencias')
        
        columns_diferences = {
            'fecha_proceso': 'FechaTexto', 'name':'BANCO', 
            'monto total':'Monto Kashio', 'Monto':'Monto Banco'
        }
        conciliacion_payouts = conciliacion_payouts.rename(columns=columns_diferences)
        
        # CORRECCIÓN AQUÍ: Uso de ffill() y bfill() directamente en lugar de fillna(method=...)
        conciliacion_payouts['FechaTexto'] = conciliacion_payouts['FechaTexto'].ffill().bfill()

        st.dataframe(conciliacion_payouts, use_container_width=True)

        payouts_metabase_df['Estado'] = f'Conciliacion_{fecha}' 

        if 'Diferencias' in conciliacion_payouts['Estado'].values:
            st.warning('Se detectaron diferencias en la conciliación')
            
            if 'Banco metabase' not in merge_op.columns:
                merge_op = merge_op.rename(columns={
                    'name_x': 'Banco estados de cuenta', 'Operación - Número': 'Numero operacion banco',
                    'Monto': 'Monto bancos', 'name_y': 'Banco metabase', 'monto total': 'Monto metabase'
                })
                
                merge_op['Banco final'] = merge_op['Banco metabase'].combine_first(merge_op['Banco estados de cuenta'])
                bancos_con_diferencias = conciliacion_payouts[conciliacion_payouts['Diferencia'] != 0]['BANCO'].unique()
                merge_op_filtrado = merge_op[merge_op['Banco final'].isin(bancos_con_diferencias)]

                with st.expander('Detalle de diferencias'):
                    st.dataframe(merge_op_filtrado.iloc[:, :7], use_container_width=True)

                diferencias_ = payouts_metabase_df['ope_psp'].isin(merge_op['Numero operacion banco'])
                payouts_metabase_df.loc[diferencias_, 'Estado'] = f'Conciliacion_{fecha} - Diferencias' 

        else:
            st.success('No se encontraron diferencias en la conciliación')

        with st.container():
            archivo_nombre = f'Conciliacion_{fecha}.xlsx'
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                payouts_metabase_df.to_excel(writer, sheet_name='Payouts_Metabase', index=False)
                df_final.to_excel(writer, sheet_name='Operaciones Bancos', index=False)

            st.download_button(
                label='DESCARGAR CONCILIACIÓN',
                data=excel_buffer.getvalue(),
                file_name=archivo_nombre,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                use_container_width=True
            )
