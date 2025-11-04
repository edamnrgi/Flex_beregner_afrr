import streamlit as st
import numpy as np
import pandas as pd
import requests
import matplotlib.pyplot as plt

from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import holidays

############## Layout ##############

# Bred streamlit-side
st.set_page_config(layout="wide")
st.title("aFRR aktiveringer")
st.markdown("""
        - Lavlast time = billigste tarif time
        - Højlast time = mellem tarif time
        - Spidslast time = dyreste tarif time
        - De aktuelle eltariffer kan findes på Energinets hjemmeside: [Energinet - Aktuelle tariffer](https://energinet.dk/el/elmarkedet/tariffer/aktuelle-tariffer/)
        - De aktuelle nettariffer kan findes på den relevante DSO's (forsyningsområde) hjemmeside:
        - Alle de nedenstående elpriser er eks. moms
        - Kurs anvendt: 1 EUR = 7,45 DKK""")

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

#######################################################################################################
# Init: Nulstil filtreret data ved rerun
if "filters_applied" not in st.session_state:
    st.session_state.df_filtered = None
    st.session_state.filters_applied = False

    st.session_state.df_filtered_2 = None # spotpriser
    st.session_state.spot_applied = False

################################################################################################################################################
############## Hent data ##############
@st.cache_data(ttl=2592000)  # 30 dage
def load_data_parquet(path):
    cols_needed = ['ActivationTime', 'PriceArea', 'aFRR_DownActivatedPriceEUR', 'aFRR_UpActivatedPriceEUR']
    df = pd.read_parquet(path, columns=cols_needed)

    # Rename til dine ønskede navne
    df.columns = ['Tid (UTC)', 'Synkronområde', 'aFRR-ned aktiveringspris (EUR)', 'aFRR-op aktiveringspris (EUR)']

    # Konverter EUR til DKK
    df["aFRR-ned aktiveringspris (DKK)"] = df["aFRR-ned aktiveringspris (EUR)"] * 7.45
    df["aFRR-op aktiveringspris (DKK)"] = df["aFRR-op aktiveringspris (EUR)"] * 7.45
    
    # Tidshåndtering
    df['Tid (UTC)'] = pd.to_datetime(df['Tid (UTC)'], utc=True)
    df['Tid (DK)'] = df['Tid (UTC)'].dt.tz_convert(ZoneInfo('Europe/Copenhagen'))
    
    return df

#df_data = load_data_parquet('./data/aFRR_aktiveringsdata_kopi.parquet')
if "df_data" not in st.session_state:
    #st.session_state.df_data = load_data_parquet('./data/aFRR_aktiveringsdata_20250627.parquet')
    #st.session_state.df_data = load_data_parquet('./data/aFRR_aktiveringsdata_kopi.parquet')
     st.session_state.df_data = load_data_parquet('./data/aFRR_aktiveringsdata_kopi.parquet')
    
df_data = st.session_state.df_data

################################################################################################################################################
############## Sidehoved filter med input fra bruger ##############
 
# --------------------------------------------
# Sidebar filtre
# --------------------------------------------
st.sidebar.header('Filtre')
with st.sidebar.form("filter_form"):
    st.subheader('Synkronområde')
    Synkronområde = st.selectbox(
        label='Vælg synkronområde',
        options=df_data['Synkronområde'].unique(),
        key="område_valg"
    )

    st.subheader('Datointerval')
    min_val = df_data['Tid (DK)'].min().date() + timedelta(days=1)
    max_val = df_data['Tid (DK)'].max().date()
    start_date = st.date_input('Start Dato', min_value=min_val, max_value=max_val, value=min_val)
    end_date = st.date_input('Slut Dato', min_value=min_val, max_value=max_val, value=max_val)
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    st.subheader('Kundetype & tariffer')
    kundetype = st.selectbox('Vælg kundetype', ['C', 'B-lav', 'B-høj', 'A-lav', 'A-høj'], key="kundetype")
    lavlast = st.number_input("Lavlast [DKK/MWh]", value=0.0)
    højlast = st.number_input("Højlast [DKK/MWh]", value=0.0)
    spidslast = st.number_input("Spidslast [DKK/MWh]", value=0.0)
    eltarif = st.number_input("Eltarif [DKK/MWh]", value=120.0)

    submitted = st.form_submit_button("Anvend filtre")

## Gem status i session_state
if submitted:
    st.session_state["filtre_anvendt"] = True

# Brug den gemte status til at styre visning
if st.session_state.get("filtre_anvendt", False):
    pass
else:
    st.markdown("*Indsæt filtre*")
    st.stop()


# --------------------------------------------
# Filtrering af data
# --------------------------------------------
if submitted:
    mask = (
        (df_data['Synkronområde'] == Synkronområde) &
        (df_data['Tid (DK)'].dt.date >= start_date) &
        (df_data['Tid (DK)'].dt.date <= end_date)
    )
    df_filtered = df_data.loc[mask].reset_index(drop=True)
    st.session_state.df_filtered = df_filtered
    st.session_state.filters_applied = True
    st.session_state.applied_filters = {
        "Synkronområde": Synkronområde,
        "Startdato": start_date,
        "Slutdato": end_date,
        "kundetype": kundetype,
        "lavlast": lavlast,
        "højlast": højlast,
        "spidslast": spidslast,
        "eltarif": eltarif
    }

# --------------------------------------------
# Spotdata (cached)
# --------------------------------------------
@st.cache_data(ttl=2592000)
def get_spotdata(Synkronområde, start_date, end_date):
    url = "https://api.energidataservice.dk/dataset/Elspotprices"

    params = {
        "filter": f'{{"PriceArea":["{Synkronområde}"]}}',
        "start": start_date.strftime("%Y-%m-%d"),
        "end": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
    }
    response = requests.get(url, params=params)
    records = response.json().get('records', [])
    df_spot = pd.DataFrame(records)
    df_spot = df_spot.iloc[::-1].reset_index(drop=True)
    df_spot["HourUTC"] = pd.to_datetime(df_spot["HourUTC"], utc=True)
    df_spot["HourDK"] = pd.to_datetime(df_spot["HourDK"]).dt.tz_localize("Europe/Copenhagen")
    return df_spot

# --------------------------------------------
# Vektoriseret tarif-beregning
# --------------------------------------------
def beregn_tarif(df, kundetype, lavlast, højlast, spidslast):
    df = df.copy()
    kolonne_0 = np.array([lavlast]*6 + [højlast]*11 + [spidslast]*4 + [højlast]*3)
    kolonne_1 = np.array([lavlast]*6 + [spidslast]*15 + [højlast]*3)
    kolonne_2 = np.array([lavlast]*6 + [højlast]*18)
    kolonne_3 = np.array([lavlast]*24)
    dk_holidays = holidays.Denmark()
    
    df['hour'] = df['HourDK'].dt.hour
    df['weekday'] = df['HourDK'].dt.weekday
    df['month'] = df['HourDK'].dt.month
    df['is_holiday'] = df['HourDK'].dt.date.isin(dk_holidays)

    if kundetype == "C":
        df['tarif'] = kolonne_0[df['hour']]
    else:
        conditions = [
            ((df['month'] >= 4) & (df['month'] <= 9)) & ((df['weekday'] >= 5) | df['is_holiday']),
            ((df['month'] >= 4) & (df['month'] <= 9)) & ((df['weekday'] < 5) & (~df['is_holiday'])),
            ((df['month'] < 4) | (df['month'] > 9)) & ((df['weekday'] >= 5) | df['is_holiday']),
            ((df['month'] < 4) | (df['month'] > 9)) & ((df['weekday'] < 5) & (~df['is_holiday']))
        ]
        choices = [kolonne_3[df['hour']], kolonne_2[df['hour']], kolonne_2[df['hour']], kolonne_1[df['hour']]]
        df['tarif'] = np.select(conditions, choices)
    
    return df

# --------------------------------------------
# Hovedvisning
# --------------------------------------------
if st.session_state.filters_applied:
    df_filtered2 = st.session_state.df_filtered.copy()
    
    # Spotdata
    if "df_spot" not in st.session_state:
        st.session_state.df_spot = get_spotdata(Synkronområde, start_date, end_date)
    df_spot = st.session_state.df_spot
    #st.dataframe(df_spot)

    # Beregn tarif
    df_spot = beregn_tarif(df_spot, kundetype, lavlast, højlast, spidslast)

    # Map spotdata til df_filtered
    df_filtered2['Tid_H'] = df_filtered2['Tid (UTC)'].dt.floor("H")
    spot_map = df_spot.set_index(df_spot['HourUTC'].dt.floor("H"))
    df_filtered2["Spotpriser (DKK)"] = df_filtered2["Tid_H"].map(spot_map["SpotPriceDKK"])
    df_filtered2["tarif"] = df_filtered2["Tid_H"].map(spot_map["tarif"])
    
    # Eltarif + strømpris
    df_spot["El-tariffer (DKK)"] = eltarif
    df_spot["Strømpris (DKK)"] = df_spot["SpotPriceDKK"] + df_spot["tarif"] + df_spot["El-tariffer (DKK)"]
    df_filtered2["El-tariffer (DKK)"] = eltarif
    df_filtered2["Strømpris (DKK)"] = df_filtered2["Spotpriser (DKK)"] + df_filtered2["tarif"] + df_filtered2["El-tariffer (DKK)"]

    st.session_state.df_filtered2 = df_filtered2
    #st.dataframe(df_filtered2)

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################
############## Rådighedspriser ############## 

@st.cache_data(ttl=2592000)
def Rådighedspriser(Synkronområde, start_date, end_date):

    url = "https://api.energidataservice.dk/dataset/AfrrReservesNordic"
    params = {"filter": f'{{"PriceArea":["{Synkronområde}"]}}',
            "start": start_date.strftime("%Y-%m-%d"),
            "end": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            }
    response = requests.get(url, params=params)
    result = response.json()
    records = result.get('records', [])
    df = pd.DataFrame(records)
    df = df.iloc[::-1].reset_index(drop=True)
    df["TimeUTC"] = pd.to_datetime(df["TimeUTC"], utc=True)
    df["TimeDK"] = pd.to_datetime(df["TimeDK"]).dt.tz_localize("Europe/Copenhagen")

    return df

if "df_kapacitet" not in st.session_state:
    st.session_state.df_kapacitet = Rådighedspriser(Synkronområde, start_date, end_date)
df_kapacitet = st.session_state.df_kapacitet

############## Layout ##############
if st.session_state.filters_applied:
    st.markdown("#### Tabel med aFRR rådighedspriser i det valgte interval")
    # rådighedspriser
    if "df_kapacitet" not in st.session_state:
        st.session_state.df_kapacitet = Rådighedspriser()
    df_kapacitet = st.session_state.df_kapacitet

    # Vis brugte filtre
    filters = st.session_state.applied_filters
    if "Synkronområde" in filters:
        #st.caption(f'Synkronområde: :green[{filters["Synkronområde"]}]')
        #st.caption(f'Startdato: :green[{filters["Startdato"]}]')
        #st.caption(f'Slutdato: :green[{filters["Slutdato"]}]')
        st.markdown(
            f"<span style='font-size:18px;'>"
            f"Synkronområde: <strong style='color:green'>{filters['Synkronområde']}</strong> | "
            f"Startdato: <strong style='color:green'>{filters['Startdato']}</strong> | "
            f"Slutdato: <strong style='color:green'>{filters['Slutdato']}</strong> | "
            f"Antal dage: <strong style='color:green'>{(filters['Slutdato']-filters['Startdato']).days+1}</strong>"
            f"</span>",
        unsafe_allow_html=True
    )

    st.dataframe(df_kapacitet)

else:
    pass

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################
############## Bygning af budprofil ##############

st.markdown("#### Design ugeprofil")

reguleringsretning = st.selectbox(label='Vælg en reguleringsretning', 
                                  options= ['aFRR-opregulering', 'aFRR-nedregulering'],
                                  index=None, 
                                  placeholder="Vælg reguleringsretning",
                                  accept_new_options=False,
                                  key="reguleringsretning")

if reguleringsretning is not None:
    st.session_state.applied_filters["Reguleringsretning"] = reguleringsretning
else:
    # fjern værdien, hvis brugeren vælger "Ingen"
    st.session_state.applied_filters.pop("Reguleringsretning", None)
    #st.stop()

kundepris_valg = st.number_input(label ="Indtast marginalpris for at drifte atkivet [DKK/MW]", 
                                 help = "Det vil sige angiv den maksimale pris på strømmen, hvorved aktivet forsat ønskes at blive driftet.",
                                 value= 0,
                                 step = 1,
                                 placeholder="Indtast værdi her")

ingen_minimum = st.checkbox(label = "Aktivet har ikke en marginalpris",
                            help = "Der skal f.eks. sættes et kryds, hvis aktivet har et fast driftsmønster uagtet hvad strømprisen er")

if ingen_minimum:
    kundepris_valg = np.nan  # Ingen filter
    #kundepris_valg = -99999  # Ingen filter

if kundepris_valg is not None:
    st.session_state.applied_filters["Minimumspris"] = kundepris_valg


st.markdown("###### Indtast positive kW-værdier ind i manuelet eller brug knappen til at indsætte samme værdi ind i hele tabellen")
# Initialiser input-tabel kun én gang
if "df_input" not in st.session_state:
    timer = [f"{h:02d}-{(h+1)%24:02d}" for h in range(24)]
    ugedage = ["Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "Lørdag", "Søndag"]
    df = pd.DataFrame(0, index=timer, columns=ugedage)
    st.session_state.df_input = df.copy() 

#st.markdown('''<div style="color: green; font-size:16px; font-weight:bold;">- Op-reguleringsbud = indsæt positive værdier </div>
#               <div style="color: red; font-size:16px; font-weight:bold;">- Ned-reguleringsbud = indsæt negative værdier </div>''', unsafe_allow_html=True)

col1, col2 = st.columns([2, 2])
with col1:
    # Værdi som brugeren kan/vil udfylde hele tabellen med
    default_value = st.number_input("Indtast en kW-værdi der skal sættes ind i hele tabellen - OBS. du kan vælge at indtaste \"0\", hvis tabellen skal cleares", value=0)
with col2:
    st.markdown('<div style="margin-top: 28px;"></div>', unsafe_allow_html=True)
    # Knap til at udfylde hele tabellen med samme værdi
    if st.button("Udfyld hele tabellen med denne værdi"):
        st.session_state.df_input.loc[:, :] = default_value
        st.rerun()

# Vis redigerbar tabel (ændringer bliver IKKE gemt endnu)
edited_df = st.data_editor(
    st.session_state.df_input,
    use_container_width=True,
    num_rows="fixed",
    disabled=["Tidspunkt"],
    key="editable_table"  # Brug en key så input bevares midlertidigt
)

## "Gem ændringer"-knap
# Sørg for default i session_state
if "df_saved" not in st.session_state:
    st.session_state.df_saved = None
if "last_saved_time" not in st.session_state:
    st.session_state.last_saved_time = None

# Callback-funktion, der gemmer data
def save_changes():
    st.session_state.df_saved = edited_df.fillna(0).copy()
    st.session_state.last_saved_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Knap med on_click
st.button("Gem ændringer", on_click=save_changes)

# Feedback / visning
if st.session_state.df_saved is not None:
    st.success(f"✅ Ændringer senest gemt kl. {st.session_state.last_saved_time}")
    st.write("#### Aktuelle gemte værdier, som kan anvendes i nedenstående beregning:")
    st.dataframe(st.session_state.df_saved)
    st.write(
        "OBS. lige pt. kan man redigere og sortere kolonnernes headers. "
        "Dette kan ikke slås fra med anvendte tabel-pakke. Brug evt. streamlit-AgGrid til at slå fra."
    )
else:
    st.info("Ingen værdier er gemt endnu. Indtast i tabellen og klik 'Gem ændringer'.")
    st.stop()

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################
############## Bestem fleksibilitetsbetaling ##############

st.markdown("#### Valg af minimumsbetaling fra fleksibilitet & aktiveringsspecifikationer")
st.markdown("###### Obs. alle fleksibilitetsbetalinger er marginalprisafregnet")

st.markdown("###### Valg af Rådighedsbud")
Rådighedsbetaling = st.number_input(label ="Valg af minimumsbetaling for at stå til rådighed [DKK/MW]", 
                                 help = "Rådighedsbetalingen bliver marginalprisafregnet",
                                 min_value=0, 
                                 max_value=100000, 
                                 value=0, 
                                 placeholder="Indtast værdi her")

if Rådighedsbetaling is not None:
    st.session_state.applied_filters["Rådighedsbetaling"] = Rådighedsbetaling

st.markdown("###### Valg af Aktiveringsbud")
Aktiveringsbetaling = st.number_input(label ="Valg af minimumsbetaling for at blive aktiveret [DKK/MWh]", 
                                 help = "Aktiveringsbetalingen bliver marginalprisafregnet",
                                 min_value=0, 
                                 max_value=100000,
                                 value=0, 
                                 placeholder="Indtast værdi her")

if Aktiveringsbetaling is not None:
    st.session_state.applied_filters["Aktiveringsbetaling"] = Aktiveringsbetaling

st.text("""OBS. den valgte aktiveringspris er en merbetaling, som lægges oven på differencen mellem den valgte marginalpris og den gældende strømpris for hver time.""")

with st.expander("Note til Thomas O + beskrivelse af beregningerne"):
    st.text("""
Alternativ skal den nuværende strømprisen ikke inkluderes i aktiveringsbudet og dermed vil værdien angivet i det ovenstående felt 1:1 være det gældende bud?
Noter at ved nedregulering der vil imbalance prisen ofte være <= spotprisen og omvendt ved opregulering. Grunden til at det kun er 'ofte' og ikke 'altid' er fordi det der mFRR der afgør imbalance retningen.
Dvs. at ultimo forventes det at diverse aktiveringer vil gøre en positiv forskel for ens imbalance omkostninger.

Beskrivelse af hvordan beregningerne bliver lavet:
    For opregulering vil aktiveringsbudet være lig med differencen mellem den valgte marginalpris og den gældende strømpris + den valgte aktiveringspris.
    Eksempler for opregulering, hvor strømprisen i en time er 100 DKK/MWh og den fastsatte marginalpris er 300 DKK/MWh: 
    - Angives en aktiveringspris på 0 DKK = så vil aktiveringsbudet der indleveres til Energinet være lig med differencen mellem den valgte marginalpris og den gældende strømpris. Dvs. 300-100 = 200kr.
    - Angives en aktiveringspris på 100 DKK = så vil aktiveringsbudet der indleveres til Energinet være lig med; 300-100+100 = 300kr.
            
    For nedregulering vil aktiveringsbudet være lig med differencen mellem den valgte marginalpris og den gældende strømpris - den valgte aktiveringspris.
    Eksempler for nedregulering, hvor strømprisen i en time er 300 DKK/MWh, den fastsatte marginalpris er 100 DKK/MWh: 
    - Angives en aktiveringspris på 100 DKK = så vil aktiveringsbudet være lig med differencen mellem den valgte marginalpris og den gældende spotpris. Dvs. 100-300-100 = -300kr.""")


st.markdown("###### Valg af aktiveringsspecifikationer")

col1, col2, col3 = st.columns(3)

with col1:
    delay = st.number_input(
                label = "Delay tid [sekunder]",
                min_value=0, 
                max_value=100000, 
                value=30, 
                step=1, 
                key="delay"
                )
    if Rådighedsbetaling is not None:
        st.session_state.applied_filters["delay"] = delay

with col2:
    ramp_up = st.number_input(
                label = "Ramp-up tid [sekunder] (aktiveringstid fra 0% til 100%)",
                min_value=0, 
                max_value=100000, 
                value=120, 
                step=1, 
                key="ramp_up"
                )
    if ramp_up is not None:
        st.session_state.applied_filters["ramp_up"] = ramp_up

with col3:
    pass


st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################
############## Beregn fleksibilitetsbetaling ##############

st.markdown("#### Beregninger")  

st.markdown("<span style='color:blue'>Noter at hvis nogle af de ovenstående parametre ændres, så forsvinder rådighedsberegningen og skal laves igen ved at trykke på knappen nedenfor</span>", unsafe_allow_html=True)
st.markdown(f"**Info:** Antal dage i det valgte datointerval = **{st.session_state.df_filtered['Tid (DK)'].dt.date.nunique()} dage**")
st.markdown(f"Fleksibilitetspotentiale på markedet for **{st.session_state.applied_filters['Reguleringsretning']}**")

def afrr_aktivering(retning, df, marginalpris, aktiveringspris):

    if np.isnan(marginalpris):
        marginalpris = 0

        if retning == "aFRR-opregulering":
            count = df[(aktiveringspris < df["aFRR-op aktiveringspris (DKK)"])].shape[0]
            df["aFRR_op"] = np.where((aktiveringspris < df["aFRR-op aktiveringspris (DKK)"]), df["aFRR-op aktiveringspris (DKK)"], np.nan)
            df["aFRR_op_strøm"] = np.where((aktiveringspris < df["aFRR-op aktiveringspris (DKK)"]), df["Strømpris (DKK)"], np.nan)
            # meromkostning for at divere fra den oprindelige driftsplan:
            df['Abs. forskel ift. marginalprisen'] = np.where((aktiveringspris < df["aFRR-op aktiveringspris (DKK)"]), 0, np.nan)
            aFRR_navn = "aFRR_op"
    
        elif retning == "aFRR-nedregulering":
            count = df[(-aktiveringspris > df["aFRR-ned aktiveringspris (DKK)"])].shape[0]
            df["aFRR_ned"] = np.where((-aktiveringspris > df["aFRR-ned aktiveringspris (DKK)"]), -df["aFRR-ned aktiveringspris (DKK)"], np.nan)
            df["aFRR_ned_strøm"] = np.where((-aktiveringspris > df["aFRR-ned aktiveringspris (DKK)"]), df["Strømpris (DKK)"], np.nan)
            # meromkostning for at divere fra den oprindelige driftsplan:
            df['Prisforskel i absolut værdi'] = np.where((-aktiveringspris > df["aFRR-ned aktiveringspris (DKK)"]), 0, np.nan)
            aFRR_navn = "aFRR_ned"
        else:
            st.warning("! Fejl ifm. valg af reguleringsretning !")

    else:
        if retning == "aFRR-opregulering":
            count = df[(marginalpris+aktiveringspris-df["Strømpris (DKK)"] < df["aFRR-op aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] < marginalpris)].shape[0]
            df["aFRR_op"] = np.where((marginalpris+aktiveringspris-df["Strømpris (DKK)"] < df["aFRR-op aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] < marginalpris), df["aFRR-op aktiveringspris (DKK)"], np.nan)
            df["aFRR_op_strøm"] = np.where((marginalpris+aktiveringspris-df["Strømpris (DKK)"] < df["aFRR-op aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] < marginalpris), df["Strømpris (DKK)"], np.nan)
            # meromkostning for at divere fra den oprindelige driftsplan:
            df['Prisforskel i absolut værdi'] = np.where((marginalpris+aktiveringspris-df["Strømpris (DKK)"] < df["aFRR-op aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] < marginalpris), (marginalpris - df['Strømpris (DKK)']).abs(), np.nan)
            aFRR_navn = "aFRR_op"
        
        elif retning == "aFRR-nedregulering":
            count = df[(marginalpris-aktiveringspris-df["Strømpris (DKK)"] > df["aFRR-ned aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] > marginalpris)].shape[0]
            df["aFRR_ned"] = np.where((marginalpris-aktiveringspris-df["Strømpris (DKK)"] > df["aFRR-ned aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] > marginalpris), -df["aFRR-ned aktiveringspris (DKK)"], np.nan)
            df["aFRR_ned_strøm"] = np.where((marginalpris-aktiveringspris-df["Strømpris (DKK)"] > df["aFRR-ned aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] > marginalpris), df["Strømpris (DKK)"], np.nan)
            # meromkostning for at divere fra den oprindelige driftsplan:
            df['Prisforskel i absolut værdi'] = np.where((marginalpris-aktiveringspris-df["Strømpris (DKK)"] > df["aFRR-ned aktiveringspris (DKK)"]) & (df["Strømpris (DKK)"] > marginalpris), (df['Strømpris (DKK)'] - marginalpris).abs(), np.nan)
            aFRR_navn = "aFRR_ned"
        else:
            st.warning("! Fejl ifm. valg af reguleringsretning !")


    return(count, aFRR_navn)

def delay_function(delay_tid, rampup_tid, df, aFRR_navn):

        # Lav en maske for, hvor aFRR_navn indeholder en værdi
        mask = df[aFRR_navn].notna()

        # Tæl hvor mange sekunder i træk aFRR_navn har været "aktiv"
        df["aktiv serie"] = mask.groupby((~mask).cumsum()).cumsum()

        # 3️⃣ Beregn aktiveringsprocent
        # - Starter ved 0 før rampup_tid sek.
        # - Stiger lineært fra 0 → 1 over delay_tid sek.
        # - Bliver 1 (100%) derefter
        df["aktivering"] = np.clip((df["aktiv serie"] - delay_tid) / rampup_tid, 0, 1)

        # 4️⃣ Beregn aktiveringsindtjening som aktiveret produkt
        df["indtjening_aktiveringer"] = (df["bud_kw"]/1000) * df[aFRR_navn] * df["aktivering"]

        # Beregn omkostninger ifm. aktiveret produkt
        df["omkostninger_aktiveringer"] = (df["bud_kw"]/1000) * df["Prisforskel i absolut værdi"] * df["aktivering"]

        # Beregn aktiveret produkt MWh
        df["aktiveret_MW"] = (df["bud_kw"]/1000) * df["aktivering"]

        return(df)


if st.button("Lav Berening"):

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### Rådighedsberegninger")

        if  st.session_state.filters_applied and "df_saved" in st.session_state and "Rådighedsbetaling" in st.session_state.applied_filters and "Reguleringsretning" in st.session_state.applied_filters:
            #st.write(st.session_state.applied_filters['Minimumspris'])
            #st.write(st.session_state.applied_filters['kundetype'])
            #st.write(st.session_state.applied_filters['Rådighedsbetaling'])
            
            if st.session_state.applied_filters['Reguleringsretning'] == "aFRR-opregulering":
                navn_reguleringsretning = "UpPriceDKK"
            elif st.session_state.applied_filters['Reguleringsretning'] == "aFRR-nedregulering":
                navn_reguleringsretning = "DownPriceDKK"
            else:
                st.warning("! Fejl ifm. valg af reguleringsretning !")
                st.stop()
            
            df_prices = df_kapacitet.copy()

            # Træk time og ugedag ud
            df_prices["hour"] = df_prices["TimeDK"].dt.hour
            df_prices["weekday"] = df_prices["TimeDK"].dt.day_name()

            # Lav interval-kode som i budtabel
            df_prices["interval"] = df_prices["hour"].apply(lambda h: f"{h:02d}-{(h+1)%24:02d}")

            # Map engelske ugedage til danske (for at matche budtabel)
            weekday_map = {
                "Monday": "Mandag", "Tuesday": "Tirsdag", "Wednesday": "Onsdag",
                "Thursday": "Torsdag", "Friday": "Fredag", "Saturday": "Lørdag", "Sunday": "Søndag"
            }
            df_prices["weekday_dk"] = df_prices["weekday"].map(weekday_map)

            # Hent budstørrelse fra 24x7 tabellen
            def hent_bud(row):
                try:
                    return st.session_state.df_saved.loc[row["interval"], row["weekday_dk"]]
                except:
                    return 0  # Hvis noget går galt, indsæt 0

            df_prices["bud_kw"] = df_prices.apply(hent_bud, axis=1)

            # Strøm prisen
            df_prices["Strømpris (DKK/MWh)"] = df_spot["Strømpris (DKK)"] 

            # Beregn indtjening i DKK
            if np.isnan(st.session_state.applied_filters['Minimumspris']): # hvis brugeren klikker "har ikke en marginalpris"
                df_prices["indtjening"] = df_prices["bud_kw"] * df_prices[navn_reguleringsretning] / 1000  # konverter kW til MW
                total = df_prices["indtjening"].sum()
                # st.success("✅ Beregning færdig!")

            elif st.session_state.applied_filters['Minimumspris'] >= -100000: # hvis brugeren har en marginalpris
                with st.spinner("Udfører hurtig vektoriseret beregning..."):
                    A = df_prices[navn_reguleringsretning].to_numpy(copy=False)
                    B = df_prices["bud_kw"].to_numpy(copy=False)
                    C = df_prices["Strømpris (DKK/MWh)"].to_numpy(copy=False)
                    D = st.session_state.applied_filters['Minimumspris']

                    if st.session_state.applied_filters['Reguleringsretning'] == "aFRR-opregulering":
                        mask = C < D
                    elif st.session_state.applied_filters['Reguleringsretning'] == "aFRR-nedregulering":
                        mask = C > D
                    else:
                        st.warning("! Fejl ifm. valg af reguleringsretning !")
                        st.stop()
                    
                    # brug float til at kunne indsætte NaN for rækker hvor mask=False
                    price_result = np.empty_like(A, dtype=np.float64)
                    price_result.fill(np.nan)
                    np.multiply(A, B, out=price_result, where=mask)

                    df_prices["indtjening"] = price_result/1000  # konverter kW til MW
                    total = df_prices["indtjening"].sum()
                    # st.success("✅ Beregning færdig!")
            else:
                st.warning("! Fejl ved indtasning af Marginalpris !")
            
            
            # Få antal unikke dage
            df_prices["Dato"] = pd.to_datetime(df_prices["TimeDK"]).dt.date
            antal_dage = df_prices["Dato"].nunique()

            # Vis resultat
            st.success(f"💰 Rådighedsindtjening i dataperiode: **{total:,.0f} DKK**")
            st.success(f"💰 Estimeret årlig rådighedsindtjening ud fra den anvendte dataperiode: **{(total*365)/antal_dage:,.0f} DKK**")

            st.write("Gennemsnitlig rådighedsindtjening, som aktivet modtager for at levere ", st.session_state.applied_filters["Reguleringsretning"], ": ", round(df_prices["indtjening"].mean(), 1), " **DKK/time**")
            st.write("Antal timer der bydes ", st.session_state.applied_filters["Reguleringsretning"], ": ", df_prices["indtjening"].count(), " i dataperioden")


            kolonner = ["TimeDK", "interval", "weekday_dk", navn_reguleringsretning, "bud_kw", "Strømpris (DKK/MWh)", "indtjening"]
            st.session_state.df_prices_subset = df_prices[kolonner]
            
            with st.expander("📊 Se tidsserien over buddata og indtjening"):
                st.dataframe(st.session_state.df_prices_subset)
            
        else:
            st.warning("! Mangler enten at anvende filtre, gemme en ugeprofilm indtaste en minimumspris for at stå til rådgihed og/eller vælge en reguleringsretning !")

    with col2:
        st.markdown("##### Aktiveringsbetalinger")

        if  st.session_state.filters_applied and "df_saved" in st.session_state and "Aktiveringsbetaling" in st.session_state.applied_filters and "Rådighedsbetaling" in st.session_state.applied_filters and delay and ramp_up:
            pass
        else:
            st.warning("! Mangler data !")
            st.stop()


        if "df_prices_subset" not in st.session_state:
            st.warning("! Mangler at lave en rådighedsberegning først !")
            st.stop()
        else:
            df_prices_subset = st.session_state.df_prices_subset
            df_aktivering = st.session_state.df_filtered2.copy()
            
            # Filtrer df2, så kun rækker med B != None/NaN er med
            df2_valid = df_prices_subset[df_prices_subset["indtjening"].notna()].copy()
            # Lav en timekolonne i df1, så den kan matches til df2["TimeDK"] 
            df_aktivering["TimeDK"] = df_aktivering["Tid (DK)"].dt.floor("H")
            # Merge kun med de "gyldige timer" fra df2
            df_aktivering = df_aktivering.merge(df2_valid[["TimeDK", "bud_kw"]], on="TimeDK", how="left")
        
        count, aFRR_navn = afrr_aktivering(st.session_state.applied_filters['Reguleringsretning'], df_aktivering, st.session_state.applied_filters["Minimumspris"], st.session_state.applied_filters["Aktiveringsbetaling"])

        df_aktivering_resultater = delay_function(delay, ramp_up, df_aktivering, aFRR_navn)

        total = df_aktivering["indtjening_aktiveringer"].sum()/3600
        aktiveret_MW = df_aktivering["aktiveret_MW"].sum()/3600
        omkost = df_aktivering["omkostninger_aktiveringer"].sum()/3600
     

        # Vis resultat
        st.success(f"💰 Aktiveringsindtjening i dataperiode: **{total:,.0f} DKK**")
        st.success(f"💰 Estimeret årlig aktiveringsindtjening ud fra den anvendte dataperiode: **{(total*365)/antal_dage:,.0f} DKK**")
        #st.markdown(f" {st.session_state.applied_filters['delay']}, {st.session_state.applied_filters['ramp_up']}")

        st.markdown(f"Antal aktiveret MWh i dataperioden = **{aktiveret_MW:,.1f} MWh**")
        st.markdown(f"""<div style='line-height:1.5; font-size:16px;'>
                        Forbrugsomkostninger forbundet med at divergere fra oprindelig driftsplan: 
                        <strong>-{omkost:,.0f} DKK</strong> i dataperioden.<br>
                        <span style='color:gray; font-size:14px;'>(Hvis aktivet **ikke** har en marginalpris, så sættes omkostningerne til 0 DKK)</span></div>""", unsafe_allow_html=True)


        #st.markdown(aFRR_navn)

        with st.expander("📊 Se tidsserien over aktiveringsdata og indtjening"):
                st.dataframe(df_aktivering_resultater)

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################

st.write("###### De nuværende anvendte filtre kan ses i tabellen herunder:")
st.table(st.session_state.applied_filters)

st.markdown("<hr style='border:2px solid black'>", unsafe_allow_html=True)

################################################################################################################################################
############## Afsluttende sidebar layout ##############
if "applied_filters" in st.session_state:
    st.sidebar.write("#### Anvendte filtre:")
    st.sidebar.dataframe(st.session_state.applied_filters, height= len(st.session_state.applied_filters) * 38)
else:
    st.sidebar.info("Ingen filtre er anvendt endnu.")


