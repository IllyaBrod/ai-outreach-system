from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chat_models import ChatOpenAI
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS, VectorStore
from langchain.chains import LLMChain
from dotenv import load_dotenv, find_dotenv
import os
import json
from bs4 import BeautifulSoup
import requests
import tiktoken
from .exceptions import PersonalizedEmailCreationException

load_dotenv(find_dotenv())

"""
1. Get CSV
2. Scrape one company website
3. Based on the content, ask llm to write email with specific possible services for that company from Eryndor
4. Send an email (for now only send to me)
5. Repeat
"""

embeddings = OpenAIEmbeddings()
llm = OpenAI(temperature=0.4, model="gpt-3.5-turbo-instruct", max_tokens=512)
llm2 = ChatOpenAI(temperature=0.0, model_name="gpt-3.5-turbo")
created = False

# Function to split text in chunks
def get_text_chunks(text, company_name):
    text_splitter = CharacterTextSplitter(
        separator=" ",
        chunk_size=1024,
        chunk_overlap=100,
        length_function=len
    )

    # chunks = text_splitter.split_text(text)
    chunks = text_splitter.create_documents([text], metadatas=[{"company": company_name}])
    return chunks

# Function to upload chunks to vector database
def update_vector_store(documents) -> VectorStore:
    if (created == False):
        global vectorstore
        vectorstore = FAISS.from_documents(documents=documents, embedding=embeddings)
        created == True
    else:
        vectorstore.add_documents(documents)
    
    return vectorstore

# A tool to access the website and scrape it
def scrape_website(url: str, company_name: str) -> str:
    print(url)
    api_key = os.getenv("BROWSERLESS_API_KEY")
    post_url = f"https://chrome.browserless.io/content?token={api_key}"

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
    }

    # Check if url has at least http
    if url[:4] != "http":
        url = "https://" + url
        print(f"Not acceptable url format: {url}. Changing it to: {url}")

    data = {
        "url": url,
    }

    data_json = json.dumps(data)

    response = requests.post(post_url, headers=headers, data=data_json)

    if (response.ok):
        soup = BeautifulSoup(response.content, "html.parser")
        content = soup.get_text()

        chunks = get_text_chunks(content, company_name)
        update_vector_store(chunks)
        result = vectorstore.similarity_search("What are " + company_name + "'s products and what is this company in general?", k=3, filter=dict(company=company_name))

        return result
    else:
        raise PersonalizedEmailCreationException(f"HTTP request to browserless failed with the status code {response.status_code}.\nResponse object: {response}")
    
def get_tokens_number(text: str):
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    num_tokens = len(encoding.encode(text))
    return num_tokens
    
# A tool to get info from scraped info and turn it in the personalized email
def write_email_text_openai(company_information, first_name, company_name):
    template = '''You are the best salesman in the world and you know how to grab people's attention. You are also a professional copy writer. And I need your help. I have an AI software development company and I am doing the cold outreach to my leads via email. I already have a cold email content written, but I want to make the email personalized, so that it feels for the prospect like I wrote it personally for them. Therefore, I need you to only create a personalized compliment/first liner about the company of my prospect. Say that I was looking for potential partners to reach out to, and then I found them, and so on. Make it REALLY personalized.
    I am providing you with the company information, as well as with the email that I need to personalize. Return the full email with the personalization added. Do not change anything else.

    Company information: {company_information}
    Email: {email}
    '''

    calendly_link = "https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor"
    email = f'''
Dear {first_name},

I hope this email finds you well! I'm Illya Brodovskyy, Founder & CEO of Eryndor. "PERSONALIZATION GOES HERE"

At Eryndor, we specialize in boosting efficiency and significantly reducing operational costs for companies like yours. We do this by building unique AI systems that are specifically tailored to our clients' needs. This way we can completely automate the processes of our customers and seamlessly integrate our solutions in their workflows. The time is the biggest asset that anyone can possess. Therefore, we want to free up your company's time and resources, so that you can allocate them to growth and innovations.

One notable example of our work involved automating Dutch FinTech startup's client onboarding. Using advanced technologies like LLM and object detection, we built a system that automatically processed, analyzed, and verified legal corporate documentation uploaded during onboarding. This system not only saves time and costs but also enhances customer satisfaction.

I'd love to have an opportunity to discuss with you how Eryndor can elevate {company_name}'s operations. Could we schedule a 30-minute call next week? Alternatively, feel free to schedule a meeting at your convenience using my Calendly link: {calendly_link}.

Looking forward to working together and contributing to {company_name}'s success.

Warm regards,
Illya Brodovskyy
CEO and Founder, Eryndor
https://www.eryndortech.com

PS. As a special offer only for November, we offer to build an MVP tailored to {company_name}'s requirements completely for FREE. This way you can experience the benefits firsthand before making any commitment.
'''

    prompt = PromptTemplate(template=template, input_variables=["company_information", "email"])

    # Calculate the tokens number of prompt
    """
    p = prompt.format_prompt(company_information=company_information, meeting_link="https://calendar.illya.eryndor.com", first_name=first_name, company_name=company_name)

    print(get_tokens_number(p.to_string()))
    """
    llm_chain = LLMChain(prompt=prompt, llm=llm)

    try:
        answer = llm_chain.run(company_information=company_information, email=email)

        return answer
    except Exception as e:
        raise PersonalizedEmailCreationException(f"There was an error during email creation with LLM.\nFull exception text: {e}")

def create_personalized_email(url: str, company_name: str, first_name: str) -> str:
    company_info = scrape_website(url, company_name)
    email = write_email_text_openai(company_info, first_name, company_name)
    print(email)

    return email



# Legacy templates
    template = """I am doing a customer outreach on behalf of my software development company called Eryndor that builds custom AI automation systems for businesses. And I want you to write a simple personalized email that would encourage recipients to read the email fully and schedule a meeting with me, Founder & CEO, Illya Brodovskyy, to discuss potential collaboration. The email should be engaging, complimentary, and avoid sounding generic or overly sales-oriented.

    I will provide you with the scraped data from the main page of the prospect company's website and I need you to write this personalized email knowing the information about the prospect's company from their website. Make it simple and do not use overcomplicated language. Compliment something about their company and say that we can help them (DO NOT COME UP WITH SPECIFIC SOLUTIONS). Really try to make it seem not generic cold email. Ask them if they would be available for a short call next week. Alternatively, paste the link to the website where prospect can schedule the meeting with me (link is below).

    Here's a glimpse of what Erydor can offer:
        - Building Customized AI Solutions: We design and build AI systems tailored to our clients' unique problem and requirements. We do not have prebuilt general solutions, we develop software explicitly for each business, so that we can make the system as specialized as possible
        - Increased Efficiency: Our solutions automate processes, saving time and resources.

    EMAIL FORMATTING:
        - NEVER ADD LINKS IN THIS FORMAT: [https://www.eryndortech.com](https://www.eryndortech.com) or [https://www.eryndortech.com]. ONLY PASTE LINKS THIS WAY: https://www.eryndortech.com.
            - GREAT EXAMPLE OF PASTING LINKS TO THE EMAIL: To schedule a meeting with me, please use this link: https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor
            - TERRIBLE EXAMPLE OF PASTING LINKS TO THE EMAIL: I invite you to schedule a meeting with me at your convenience using the following link: [https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor](https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor)
        - DO NOT INCLUDE SUBJECT OF THE EMAIL
        - AGAIN, DO NOT ADD SUBJECT

    EMAIL CONTENT:
        - IMPORTANT, DO NOT SAY THAT WE HAVE WORKED WITH MANY COMPANIES. NEVER SAY THAT WE HAVE HELPED NUMEROUS COMPANIES OR THAT WE HAVE PROVEN TRACK RECORD. FOLLOW THIS RULE STRICTLY
        - INTRODUCE ILLYA BRODOVSKYY (ME) AT THE BEGINNING
        - FOLLOW ALREADY MENTIONED INSTRUCTIONS FOR THE CONTENT 
    

    RECIPIENT'S FIRST NAME: {first_name}

    RECIPIENT'S COMPANY NAME: {company_name}
    
    INFORMATION ABOUT {company_name}: {company_information}

    SCHEDULE MEETING LINK: {meeting_link}

    MY NAME (FOUNDER & CEO): Illya Brodovskyy

    ERYNDOR'S COMPANY WEBSITE: https://www.eryndortech.com
    """

    template2 = """I am doing a customer outreach on behalf of my software development company called Eryndor that builds custom AI automation systems for businesses. And I want you to write a simple personalized email that would encourage recipients to read the email fully and schedule a meeting with me, Founder & CEO, Illya Brodovskyy, to discuss potential collaboration. The email should be engaging, complimentary, and avoid sounding generic or overly sales-oriented.

    I will provide you with the scraped data from the main page of the prospect company's website and I need you to write this personalized email knowing the information about the prospect's company from their website. Make it simple and do not use overcomplicated language. Compliment something about their company and say that we can help them (DO NOT COME UP WITH SPECIFIC SOLUTIONS). Ask them if they would be available for a short call next week and when. Alternatively, paste the link to the website where prospect can schedule the meeting with me (link is below).

    A glimpse of what Erydor can offer:
        - Building Customized AI Solutions: We design and build AI systems tailored to our clients' unique problem and requirements. We do not have prebuilt general solutions, we develop software explicitly for each business, so that we can make the system as specialized as possible.
        - Increased Efficiency: Our solutions automate processes, saving time and resources.

    Value proposition of Eryndor:
        -Measurable Efficiency Gains:

            Experience a quantifiable boost in efficiency as our tailored solutions optimize your workflows. After implementing our solutions in your workflows, you will immediately see the results, because they can be easily measured. These measurable gains translate directly into reduced costs and enhanced productivity.

        -Liberating Resources for Growth:

            Our problem-specific AI solutions liberate your workforce from the shackles of day-to-day operations. By automating routine tasks, your team can refocus their expertise on expanding your service offerings, improving customer experiences, or delving into innovative financial strategies.

        -Unleash Your Potential:

            Imagine having the freedom to dedicate your resources to strategic initiatives, launching new financial products, or strengthening client relationships. Eryndor's tailored automation solutions not only save your resources but unlock your true potential, empowering you to thrive in a rapidly evolving financial landscape.
    

    IMPORTANT RULES:
        - NEVER ADD LINKS IN THIS FORMAT: [https://www.eryndortech.com](https://www.eryndortech.com) or [https://www.eryndortech.com]. ONLY PASTE LINKS THIS WAY: https://www.eryndortech.com.
            - GREAT EXAMPLE OF PASTING LINKS TO THE EMAIL: To schedule a meeting with me, please use this link: https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor
            - TERRIBLE EXAMPLE OF PASTING LINKS TO THE EMAIL: I invite you to schedule a meeting with me at your convenience using the following link: [https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor](https://calendly.com/illya-brodovskyy/introduction-meeting-with-eryndor)
        - DO NOT INCLUDE SUBJECT OF THE EMAIL
        - AGAIN, DO NOT ADD SUBJECT
        - WE HAVE NOT WORKED WITH MANY COMPANIES, SO DO NOT SAY ANYTHING ABOUT THAT
        - INTRODUCE ILLYA BRODOVSKYY (ME) AT THE BEGINNING
        - AT THE END OF THE EMAIL EXPLICITLY ADD "P.S." AND SAY THAT WE WILL BUILD AN MVP COMPLETELY FOR FREE, SO THAT THEY CAN SEE THE POTENTIAL BEFORE PAYING
        - FOLLOW ALREADY MENTIONED INSTRUCTIONS FOR THE CONTENT 

    INFORMATION:
        RECIPIENT'S FIRST NAME: {first_name}

        RECIPIENT'S COMPANY NAME: {company_name}
        
        INFORMATION ABOUT {company_name}: {company_information}

        ADDITIONAL INFORMATION ABOUT ERYNDOR: "At Eryndor, we're on a mission to revolutionize the way businesses operate worldwide. Our vision is clear: to nurture a culture of innovation and accelerate their growth trajectory.

🌟 Our Mission:
At Eryndor, we see a future where businesses don't just survive; they thrive through innovation. We're here to help companies to break free from the old ways of doing things and embrace a new era of business. How do we achieve this? By crafting customized, problem-specific AI-powered systems and making AI accessible to businesses of all sizes. This custom-built AI systems for specific problems focus on automating existing processes of businesses, which leads to the saved operational costs and improved efficiency of our clients."

        SCHEDULE MEETING LINK: {meeting_link}

        MY NAME (FOUNDER & CEO): Illya Brodovskyy

        ERYNDOR'S COMPANY WEBSITE: https://www.eryndortech.com
    """