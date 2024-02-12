import streamlit as st
from streamlit.delta_generator import DeltaGenerator


def background_color_table(value: float):
    bgcolor = "darkred" if value < 0 else "darkgreen"
    return f"background-color: {bgcolor};"


def remove_white_space() -> DeltaGenerator:
    return st.markdown(
        """
        <style>
                .block-container {
                    padding-top: 2rem;
                    padding-bottom: 0rem;
                    padding-left: 0rem;
                    padding-right: 0rem;
                }
        </style>
        """,
        unsafe_allow_html=True,
    )

def wave_background():
    with open('./.streamlit/wave.css') as f:
        css = f.read()
    st.markdown(f'<style>{css}</style>', unsafe_allow_html=True)
    
def css_button():
    st.markdown("""
        <style>
            div.stButton > button:first-child { 
            border-radius: 0%;
            transition-duration: 0.4s;
            }
            div.stButton:hover > button:first-child { 
            background-color: #FFFFFF; 
            color: #0f62ab;
            }
        </style>""",
    unsafe_allow_html=True,
)

def css_tabs():
    st.markdown("""
        <style>
            .stTabs [data-baseweb="tab"] {
                }
        </style>""", 
    unsafe_allow_html=True)