import os
from fastapi import FastAPI
from pydantic import BaseModel, Schema
from pydantic.dataclasses import dataclass
from typing import List
from datetime import datetime
import asyncio
import pandas as pd
import time
import redis

app = FastAPI()

BASE_DIR = "/data"

@app.get("/group/{user_id}")
def get_group(user_id):
  basic_df = pd.DataFrame()
  for filename in os.listdir(BASE_DIR):
    if not ".csv" in filename:
      continue
    elif filename.split("_")[1] in ["guess", "basic", "evaluation", "task"]:
      print(filename)
      basic_df_tmp = pd.read_csv(os.path.join(BASE_DIR, filename), index_col="user_id")
      basic_df = basic_df_tmp.combine_first(basic_df)
  
  new_gender = basic_df.loc[int(user_id), "gender"] == "male"
  new_age = basic_df.loc[int(user_id), "age"]
  new_weight = basic_df.loc[int(user_id), "weight_guess"]
  new_height = basic_df.loc[int(user_id), "height_guess"]

  exclude_index = basic_df.index.isin([int(user_id)])
  basic_df = basic_df[~exclude_index]

  c_cnt = basic_df["gender"][basic_df["group"]=="Control"].count()
  t_cnt = basic_df["gender"][basic_df["group"]=="Test"].count()

  c_gender = (basic_df["gender"][basic_df["group"]=="Control"]=="male").mean()
  t_gender = (basic_df["gender"][basic_df["group"]=="Test"]=="male").mean()
  
  c_age = basic_df["age"][basic_df["group"]=="Control"].mean()
  t_age = basic_df["age"][basic_df["group"]=="Test"].mean()
  
  c_weight = basic_df["weight_guess"][basic_df["group"]=="Control"].mean()
  t_weight = basic_df["weight_guess"][basic_df["group"]=="Test"].mean()
  
  c_height = basic_df["height_guess"][basic_df["group"]=="Control"].mean()
  t_height = basic_df["height_guess"][basic_df["group"]=="Test"].mean()
  
  cnt_div_t = abs(c_cnt - (t_cnt + 1))
  cnt_div_c = abs((c_cnt + 1) - t_cnt)

  gender_div_t = abs(c_gender - (t_gender * t_cnt + new_gender) / (t_cnt + 1))
  gender_div_c = abs((c_gender * c_cnt + new_gender) / (c_cnt + 1) - t_gender)

  age_div_t = abs(c_age - (t_age * t_cnt + new_age) / (t_cnt + 1))
  age_div_c = abs((c_age * c_cnt + new_age) / (c_cnt + 1) - t_age)

  weight_div_t = abs(c_weight - (t_weight * t_cnt + new_weight) / (t_cnt + 1))
  weight_div_c = abs((c_weight * c_cnt + new_weight) / (c_cnt + 1) - t_weight)

  height_div_t = abs(c_height - (t_height * t_cnt + new_height) / (t_cnt + 1))
  height_div_c = abs((c_height * c_cnt + new_height) / (c_cnt + 1) - t_height)

  div_t = cnt_div_t / 3 + gender_div_t * 7 + age_div_t + weight_div_t / 1.5 + height_div_t / 1.5
  div_c = cnt_div_c / 3 + gender_div_c * 7 + age_div_c + weight_div_c / 1.5 + height_div_c / 1.5

  if div_c < div_t:
    return {"group": "control"}
  else:
    return {"group": "test"}


class Tracking(BaseModel):
  userID: int
  task: int
  language: str
  group: str
  machineLayout: List[str]
  trackings: List[str]


async def join_async(string_base, string_list):
  string_return = string_base.join(string_list) + string_base
  return string_return

async def create_csv_async(filename, csv_rows):
  csv_content = await join_async("\n", csv_rows)
  with open(os.path.join(BASE_DIR, filename), "w+") as file:
    file.write(csv_content)

@app.post("/tracking")
async def save_trackings(tracking: Tracking):
  #timestamp to mitigate file overwriting
  now = datetime.now()
  timestamp = "-".join([str(now.year), str(now.month), str(now.day), str(now.hour), str(now.minute), str(now.second)])

  print("Recieved new info at {}\n--user: {}\n--task: {}\n--group:{}\n--lang:{}".format(timestamp,
                                                                                    tracking.userID,
                                                                                    tracking.task,
                                                                                    tracking.group,
                                                                                    tracking.language))

  #create csv file for the machine layout
  filename = "{}_{}_{}_machineLayout_{}.csv".format(tracking.userID, tracking.task, tracking.group, timestamp)
  task_machineLayout = asyncio.create_task(create_csv_async(filename, tracking.machineLayout))
  shielded_task_mL = asyncio.shield(task_machineLayout)

  #Create csv file for the trackings
  filename = "{}_{}_{}_trackings_{}.csv".format(tracking.userID, tracking.task, tracking.group, timestamp)
  task_trackings = asyncio.create_task(create_csv_async(filename, tracking.trackings))
  shielded_task_tr = asyncio.shield(task_trackings)

  #Todo create a video of the path that the user took
  await task_machineLayout
  if not task_machineLayout.exception() == None:
    print(task_machineLayout.exception())
  await task_trackings
  if not task_trackings.exception() == None:
    print(task_trackings.exception())
  return {"response": "success"}

class HiddenAdminValues(BaseModel):
  language: str = None
  user_id: str = None
  group: str = None

#class FieldChoice(BaseModel):
#  id: str
#  label: str
#  ref: str

#class FieldProperties(BaseModel):
#  choices: List[FieldChoice]
#  allow_multiple_selection: bool
#  allow_other_choice: bool

class Field(BaseModel):
  id: str
  title: str = None
  type: str
#  ref: str
#  properties: FieldProperties = None

class Definition(BaseModel):
  #id: str
  #title: str
  questions: List[Field] = Schema([], alias='fields')
  #hidden: List[str]

class Choice(BaseModel):
  label: str = ""

class Choices(BaseModel):
  labels: List[str] = None

class Answer(BaseModel):
  type: str
  choice: Choice = None
  choices: Choices = None
  text: str = None
  number: int = None
  question: Field = Schema(None, alias='field')

class FormResponse(BaseModel):
  #form_id: str
  #landed_at: str
  #submitted_at: str
  hidden: HiddenAdminValues = None
  definition: Definition
  answers: List[Answer]

class TypeformPayload(BaseModel):
  #event_id: str
  #event_type: str
  form_response: FormResponse

@app.post("/typeform/{step}")
async def save_typeform_survey(step, payload: TypeformPayload):
  now = datetime.now()
  timestamp = "-".join([str(now.year), str(now.month), str(now.day), str(now.hour), str(now.minute), str(now.second)])

  header = []
  values = []

  q_id_dict = {
    "fd2RZAQ92LwL":"user_id",
    "us6cowmLSpqC":"language",
    "N4BgdvOvMDd4":"weight_guess",
    "f4uxx985fXO3":"height_guess",
    "mvCGohGGtJJs":"gender",
    "ANEkf7up6lO4":"gender",
    "YqZit4V3JnNI":"age",
    "FfS6ByGviR2w":"age",
    "tYkANiSMQhHK":"education",
    "zYhZb7roQrYO":"education",
    "ApbFMPJforlD":"snack_frequency",
    "tnfl2FOpFBn8":"snack_frequency",
    "rm8EtBg5q8qx":"ar_frequency",
    "UBC7mOl9LPpO":"ar_frequency",
    "uUTub569gl8M":"user_id",
    "FppYTfu2qOUe":"group",
    "keyyc8BP77Ud":"language",
    "aWlNUCPyqAKS":"t_1",
    "bjSkpKUKgrzJ":"t_2",
    "hDxO5vsiABGK":"t_3",
    "jMo4userZWef":"t_4",
    "jcruLQD1jtsb":"IE1",
    "lQeuANlzabZO":"IE1",
    "xdCMMXgxnem1":"SI1",
    "cpFz6u4Miulb":"SI1",
    "eaTgLd8mTqIl":"IE2",
    "L90PEG9f4W3p":"IE2",
    "bNXJAnStwDfX":"EE1",
    "MEMNKBeL1Yx1":"EE1",
    "q0mA3PRRFjx7":"PE1",
    "VuQerOk1gweh":"PE1",
    "JYEh0RF8Fm8b":"HM1",
    "c9Dw9B0KswPW":"HM1",
    "WeELc4DWjE6P":"FL3",
    "V8lCzzTn6BRf":"FL3",
    "GfV0SwI2TmuK":"BI2",
    "lyboW2WkS3lJ":"BI2",
    "QVzNIkgWgGxB":"PI2",
    "Pdv12x3s88Uj":"PI2",
    "QVMeswBQSWAi":"EE3",
    "G4zCsxt5ujLA":"EE3",
    "HNBvOMYBB0aG":"PE3",
    "zpEpkexg0kg3":"PE3",
    "xUlfUW6JGEav":"SI3",
    "QnPCooEP9wLm":"SI3",
    "b4YNQSqEHFaE":"BI1",
    "Ek83Q8WpFJIK":"BI1",
    "Y4v77TAeZzKs":"PI1",
    "Vph6jKGJqd2H":"PI1",
    "erPaRi4mPyPG":"EE2",
    "MvYdT8yr5CJL":"EE2",
    "PEWOeMEEayNA":"BI3",
    "Knn4XXYAXDkh":"BI3",
    "DuGG9VdyhxCd":"HM2",
    "ztFU1N2uo1cV":"HM2",
    "Wiq2wP97n7RO":"FL1",
    "xtTPpLEKJ1a6":"FL1",
    "sBItcnzLbeab":"PE2",
    "WKGyYTm1SXGd":"PE2",
    "BQXqCdJgdxle":"PI3",
    "AedSxGtELKPw":"PI3",
    "wfA9uqPz8cRt":"SI2",
    "zfhxooYmpjqI":"SI2",
    "zDVqi1Ti9Nwq":"FL2",
    "ejbVOBOo8mor":"FL2",
    "sQO3OzQCqxV0":"colors1",
    "AOAvK7iBrh4V":"colors2",
    "eLaerlPJtb85":"colors1",
    "z53SflcCNkzP":"colors2",
    "DF0piBeuVzuj":"weight",
    "ylLRc5OsKsiw":"weight",
    "BIq0vwxQCtbT":"height",
    "HMuAhpqZqEYG":"height",
    "B6JXMrdqFECM":"diet",
    "IVU6MjgXFeCa":"diet",
  }

  c_id_dict = {
    "160cm":"0",
    "170cm":"1",
    "180cm":"2",
    "190cm":"3",
    "200cm":"4",
    "skinny":"0",
    "slim":"1",
    "normal":"2",
    "wide":"3",
    "fat":"4",
    "Male": "male",
    "Männlich": "male",
    "Female": "female",
    "Weiblich": "female",
    "Intersexual": "inter",
    "Intersexuell": "inter",
    "19 or younger": "0", #<19
    "19 oder jünger":"0", #<19
    "20 - 29":"1", #20-29
    "30 - 39":"2", #30-39
    "40 - 49":"3", #40-49
    "50 - 64":"4", #50-64
    "65 - 79":"5", #65-79
    "80 or older":"6", #>80
    "80 oder älter":"6", #>80
    "39 kg or less":"39-", #<39
    "39 kg oder weniger":"39-", #<39
    "40 - 49 kg":"40-49", #40-49
    "50 - 59 kg":"50-59", #50-59
    "60 - 69 kg":"60-69", #60-69
    "70 - 79 kg":"70-79", #70-79
    "80 - 89 kg":"80-89", #80-89
    "90 - 99 kg":"90-99", #90-99
    "100 - 109 kg":"100-109", #100 - 109
    "110 - 119 kg":"110-119", #110 - 119
    "120 - 129 kg":"120-129", #120 - 129
    "130 - 139 kg":"130-139", #130 - 139
    "140 - 149 kg":"140-149", #140 - 149
    "150 kg oder mehr":"150+", #>150
    "150 kg or more":"150+", #>150
    "139 cm or less":"139-", #<139
    "139 cm oder weniger":"139-", #<139
    "140 - 149 cm":"140-149", #140-149
    "150 - 159 cm":"150-159", #150-159
    "160 - 169 cm":"160-169", #160-169
    "170 - 179 cm":"170-179", #170-179
    "180 - 189 cm":"180-189", #180-189
    "190 - 199 cm":"190-199", #190-199
    "200 - 209 cm":"200-209", #190-199
    "210cm or more":"210+", #>200
    "210cm oder mehr":"210+", #>200
  }

  for answer in payload.form_response.answers:
    if answer.question.id in q_id_dict:
      header.append(q_id_dict[answer.question.id])
    else:
      print("unknown question {}".format(answer.question.id))
      header.append(answer.question.id)
    
    if answer.type == "text":
      values.append(answer.text)
    elif answer.type == "choice":
      if header[-1] in ["weight_guess","height_guess","gender", "age", "weight", "height"]:
        if answer.choice.label in c_id_dict:
          values.append(c_id_dict[answer.choice.label])
        else:
          values.append(answer.choice.label)
      else:
        values.append(answer.choice.label)
    elif answer.type == "choices":
      values.append(",".join(answer.choices.labels))
    elif answer.type == "number":
      values.append(str(answer.number))
    
    

  if "basic" in step:
    user_id = payload.form_response.hidden.user_id
    header.append("user_id")
    values.append(user_id)
    language = payload.form_response.hidden.language
    header.append("language")
    values.append(language)
  elif "evaluation" in step:
    user_id = payload.form_response.hidden.user_id
    header.append("user_id")
    values.append(user_id)
    language = payload.form_response.hidden.language
    header.append("language")
    values.append(language)
    group = payload.form_response.hidden.group
    header.append("group")
    values.append(group)
  elif "task" in step:
    language = values[header.index("language")]
    user_id = values[header.index("user_id")]
  elif "guess" in step:
    language = values[header.index("language")]
    user_id = values[header.index("user_id")]
  else:
    return {"response", "unknown survey type"}

  print("got new {} survey input for {} at {}".format(step, user_id, timestamp))

  survey_csv = [";".join(header), ";".join(values)]
  
  filename = "{}_{}_{}.csv".format(user_id, step, timestamp)
  task_survey = asyncio.create_task(create_csv_async(filename, survey_csv))
  shielded_task_s = asyncio.shield(task_survey)

  if any(st == step for st in ["basic", "evaluation"]):
    questions_csv = ["question.id; question.text,; question.type"]
    for question in payload.form_response.definition.questions:
      questions_csv.append("{}; {}; {}".format(question.id, question.title, question.type))
    
    filename = "{}_questionlayout-{}_{}.csv".format(user_id, step, timestamp)
    task_questions = asyncio.create_task(create_csv_async(filename, questions_csv))
    shielded_task_q = asyncio.shield(task_questions)

  await task_survey
  if not task_survey.exception() == None:
    print(task_survey.exception())

  if any(st == step for st in ["basic", "evaluation"]):
    await task_questions
    if not task_questions.exception() == None:
      print(task_questions.exception())

  return {"response": "success"}
