import untangle, sys, requests

url = ""


def getClientId():
    response = requests.get("http://localhost:8080/nifi-api/flow/client-id")
    clientId = response.content
    return clientId


def deploy_template(filename):
    # Get a client id to perform some requests
    clientId = getClientId()
    # Get process group root
    processGroupId = getProcessGroupRoot()

    # p = untangle.parse(filename)
    # new_template_name = p.template.name.cdata
    # # Get all templates
    # r = requests.get("%s/nifi-api/flow/templates" % url,
    #                  headers={"Accept": "application/json"})
    # # Check if the new template already exist
    # for each in r.json()["templates"]:
    #     if each["template"]["name"] == new_template_name:
    #         requests.delete(each["template"]["uri"])
    #
    # # Remove old control services
    # r = requests.get("%s/nifi-api/flow/process-groups/%s/controller-services" % (url, processGroupId))
    # for each in r.json()["controllerServices"]:
    #     requests.delete(each["uri"] + "?version=0")

    templateId = uploadTemplate(filename, processGroupId)  # Upload template
    instantiateTemplate(templateId, processGroupId)  # Instantiate it
    serviceId = getControllerServiceId(processGroupId)  # Get control service
    run(processGroupId, serviceId, clientId)  # Start and run


def run(groupId, serviceId, clientId):
    headers = {"Host": "localhost:8080",
               "Connection": "keep-alive",
               "Content-Length2": 98,
               "Accept": "application/json, text/javascript, */*; q=0.01",
               "X-Requested-With": "XMLHttpRequest",
               "Content-Type": "application/json",
               "Accept-Encoding": "gzip, deflate, br"}

    # get version for the current controller service
    response = requests.get("%s/nifi-api/controller-services/%s" % (url, serviceId))
    version = int(response.json()["revision"]["version"])

    # enable controller service
    data = '{"revision":{"clientId":"%s","version":%d},"component":{"id":"%s",' \
           '"state":"ENABLED"}}}' % (clientId, version, serviceId)
    response = requests.put("%s/nifi-api/controller-services/%s" % (url, serviceId), data=data,
                            headers=headers)
    print response.content

    # enable component for the controller
    data = '{"id":"%s","state":"ENABLED","referencingComponentRevisions":{}}' % serviceId
    response = requests.put("%s/nifi-api/controller-services/%s/references" % (url, serviceId),
                            data=data, headers=headers)
    print response.content

    # set the process group on running
    data = '{"id":"%s","state":"RUNNING"}' % groupId
    response = requests.put("%s/nifi-api/flow/process-groups/%s" % (url, groupId), data=data, headers=headers)
    print response.content


def uploadTemplate(filename, processGroupId):
    response = requests.post("%s/nifi-api/process-groups/%s/templates/upload" % (url, processGroupId),
                             files={"template": open(filename, 'rb')})

    # Get template's id from response
    xml = untangle.parse(response.content)
    return xml.templateEntity.template.id.cdata


def getProcessGroupRoot():
    response = requests.get("%s/nifi-api/flow/process-groups/root" % url)
    content = response.json()
    return content["processGroupFlow"]["id"]


def instantiateTemplate(templateId, processGroupId):
    data = "{ \"originX\": 0.0, \"originY\": 0.0, \"templateId\": \"%s\" }" % templateId
    requests.post("%s/nifi-api/process-groups/%s/template-instance" % (url, processGroupId), data=data,
                  headers={"Content-Type": "application/json"})


def getControllerServiceId(groupId):
    response = requests.get("%s/nifi-api/flow/process-groups/%s/controller-services" % (url, groupId))
    json = response.json()

    controllerServices = json["controllerServices"]
    for service in controllerServices:
        component = service["component"]
        if component["parentGroupId"] == groupId and component["name"] == "CSVReader":
            return service["id"]


if __name__ == "__main__":
    url = "http://localhost:8080"
    deploy_template("csvToParquet.xml")
