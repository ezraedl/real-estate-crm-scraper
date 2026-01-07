"""
Example server integration for OpenAI Agent Lookup Library

This file demonstrates how to integrate the library into various server frameworks.
"""

# FastAPI Example
def fastapi_example():
    """
    FastAPI server integration example.
    
    Install: pip install fastapi uvicorn
    Run: uvicorn server_example:fastapi_app --reload
    """
    from fastapi import FastAPI, HTTPException
    from openai_agent_lookup import AgentLookupClient
    from pydantic import BaseModel
    
    app = FastAPI(title="Agent Lookup API")
    client = AgentLookupClient()
    
    class LookupRequest(BaseModel):
        query: str
    
    class LookupResponse(BaseModel):
        agent_name: str
        mobile_numbers: list[dict]
        success: bool
    
    @app.post("/api/lookup", response_model=LookupResponse)
    async def lookup_agent(request: LookupRequest):
        """Look up agent mobile numbers"""
        try:
            result = await client.lookup_async(request.query)
            return LookupResponse(
                agent_name=result.agent_name,
                mobile_numbers=[
                    {
                        "number": m.number,
                        "certainty": m.certainty
                    }
                    for m in result.mobile_numbers
                ],
                success=True
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/api/lookup/{agent_name}/{location}")
    async def lookup_agent_path(agent_name: str, location: str):
        """Look up agent by path parameters"""
        query = f"{agent_name}, {location}"
        result = await client.lookup_async(query)
        return result.model_dump()
    
    return app


# Flask Example
def flask_example():
    """
    Flask server integration example.
    
    Install: pip install flask
    Run: flask run
    """
    from flask import Flask, jsonify, request
    from openai_agent_lookup import AgentLookupClient
    import asyncio
    
    app = Flask(__name__)
    client = AgentLookupClient()
    
    @app.route('/api/lookup', methods=['POST'])
    def lookup_agent():
        """Look up agent mobile numbers"""
        data = request.json
        query = data.get('query') if request.is_json else request.form.get('query')
        
        if not query:
            return jsonify({"error": "query is required"}), 400
        
        try:
            result = client.lookup(query)
            return jsonify({
                "agent_name": result.agent_name,
                "mobile_numbers": [
                    {
                        "number": m.number,
                        "certainty": m.certainty
                    }
                    for m in result.mobile_numbers
                ],
                "success": True
            })
        except Exception as e:
            return jsonify({"error": str(e), "success": False}), 500
    
    @app.route('/api/lookup/<agent_name>/<location>', methods=['GET'])
    def lookup_agent_path(agent_name: str, location: str):
        """Look up agent by path parameters"""
        query = f"{agent_name}, {location}"
        try:
            result = client.lookup(query)
            return jsonify(result.model_dump())
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    return app


# Django Example (views.py snippet)
def django_example():
    """
    Django integration example.
    
    Add to your views.py file.
    """
    from django.http import JsonResponse
    from django.views import View
    from django.views.decorators.csrf import csrf_exempt
    from django.utils.decorators import method_decorator
    from openai_agent_lookup import AgentLookupClient
    import asyncio
    
    @method_decorator(csrf_exempt, name='dispatch')
    class AgentLookupView(View):
        """Django view for agent lookup"""
        
        async def post(self, request):
            """Handle POST request"""
            query = request.POST.get('query') or request.body.decode('utf-8')
            
            if not query:
                return JsonResponse({"error": "query is required"}, status=400)
            
            try:
                client = AgentLookupClient()
                result = await client.lookup_async(query)
                return JsonResponse({
                    "agent_name": result.agent_name,
                    "mobile_numbers": [
                        {
                            "number": m.number,
                            "certainty": m.certainty
                        }
                        for m in result.mobile_numbers
                    ],
                    "success": True
                })
            except Exception as e:
                return JsonResponse({"error": str(e), "success": False}, status=500)
    
    return AgentLookupView


# Usage example
if __name__ == "__main__":
    # FastAPI
    fastapi_app = fastapi_example()
    
    # To run:
    # uvicorn server_example:fastapi_app --reload
    
    # Flask
    flask_app = flask_example()
    
    # To run:
    # flask_app.run(debug=True)

