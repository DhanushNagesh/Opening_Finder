FROM python:3.11-slim

RUN useradd -m -u 1000 user
USER user
WORKDIR /home/user/app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt
ENV PATH=/home/user/.local/bin:$PATH

COPY --chown=user finder.py streamlit_app.py ./
COPY --chown=user data ./data

EXPOSE 7860
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=7860", "--server.address=0.0.0.0", "--server.headless=true"]
