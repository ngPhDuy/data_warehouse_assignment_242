from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from langchain.chat_models import init_chat_model
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from memory import memory, config
from langchain import hub
from pydantic import BaseModel
import os

os.environ["LANGSMITH_API_KEY"] = "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"
os.environ["LANGSMITH_TRACING"] = "true"
os.environ["GROQ_API_KEY"] = "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"

class Request(BaseModel):
    query: str

db = SQLDatabase.from_uri("sqlite:///job_analysis.db")
model = init_chat_model("llama3-70b-8192", model_provider="groq")
sqlToolkit = SQLDatabaseToolkit(db=db, llm=model)
tools = sqlToolkit.get_tools()
prompt_template = hub.pull("langchain-ai/sql-agent-system-prompt")
system_message = prompt_template.format(dialect=db.dialect, top_k=5)
agent_executor = create_react_agent(model, tools, prompt=system_message, checkpointer=memory)


app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

    
@app.post("/request")
async def chat(request: Request):
    messages = [
        HumanMessage(content=request.query),
    ]
    answer = ""
    print("////-----------------------------------------------------------------////")
    print("QUERY STARTED")
    for step in agent_executor.stream({"messages": messages}, config, stream_mode="values"):
        step["messages"][-1].pretty_print()
        answer = step["messages"][-1].content
    print("QUERY ENDED")
    print("////-----------------------------------------------------------------////")
    return {"answer": answer}