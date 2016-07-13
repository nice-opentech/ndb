#include "ndb/options.h"

namespace ndb {

size_t HandleGet(char* buf, size_t size, size_t nmemb, void* userdata) {
  std::string* data = (std::string*) userdata;
  data->assign(buf, size * nmemb);
  return data->size();
}

Result HTTPGet(const std::string& url, std::string* data) {
  auto curl = curl_easy_init();
  if (curl == NULL) {
    return Result::Error("Init curl failed.");
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) data);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HandleGet);

  auto res = curl_easy_perform(curl);
  long code;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    return Result::Error("GET %s: %s", url.c_str(), curl_easy_strerror(res));
  }
  if (code / 100 != 2) {
    return Result::Error("GET %s: %ld", url.c_str(), code);
  }
  return Result::OK();
}

Result HTTPPut(const std::string& url, const std::string& data) {
  auto curl = curl_easy_init();
  if (curl == NULL) {
    return Result::Error("Init curl failed.");
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");

  auto res = curl_easy_perform(curl);
  long code;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
  curl_easy_cleanup(curl);

  if (res != CURLE_OK) {
    return Result::Error("PUT %s: %s", url.c_str(), curl_easy_strerror(res));
  }
  if (code / 100 != 2) {
    return Result::Error("PUT %s: %ld", url.c_str(), code);
  }
  return Result::OK();
}

std::string Center::FormatURL(const char* fmt, ...) const {
  char url[1024];
  auto n = snprintf(url, sizeof(url), "http://%s/v2/keys/clusters/%s/ndb/",
                    options_.address.c_str(), options_.cluster.c_str());
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(url + n, sizeof(url) - n, fmt, ap);
  va_end(ap);
  return url;
}

Result Center::GetMaster(std::string* address) {
  auto url = FormatURL("nodes/%s/master", options_.slaveof.c_str());
  std::string data;
  NDB_TRY(HTTPGet(url, &data));

  rapidjson::Document d;
  if (d.Parse(data.c_str()).HasParseError()) {
    return Result::Error("Parse JSON failed.");
  }

  if (!d.IsObject() ||
      !d.HasMember("node") ||
      !d["node"].HasMember("value") ||
      !d["node"]["value"].IsString()) {
    return Result::Error("Invalid JSON.");
  }

  rapidjson::Document v;
  if (v.Parse(d["node"]["value"].GetString()).HasParseError()) {
    return Result::Error("Parse JSON failed.");
  }

  if (!v.IsObject() ||
      !v.HasMember("address") ||
      !v["address"].IsString()) {
    return Result::Error("Invalid JSON");
  }

  *address = v["address"].GetString();
  return Result::OK();
}

Result Center::PutNode(const std::string& url, const std::string& address) {
  rapidjson::Value v;
  v.SetString(address.data(), address.size());
  rapidjson::Document d;
  d.SetObject();
  d.AddMember("address", v, d.GetAllocator());

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  d.Accept(writer);

  char data[1024];
  snprintf(data, sizeof(data), "value=%s", buffer.GetString());

  return HTTPPut(url, data);
}

Result Center::PutMaster(const std::string& address) {
  auto url = FormatURL("nodes/%s/master", options_.node.c_str());
  return PutNode(url, address);
}

Result Center::PutSlaves(const std::string& address) {
  auto url = FormatURL("nodes/%s/slaves/%s", options_.slaveof.c_str(), options_.node.c_str());
  return PutNode(url, address);
}

Result Center::GetOptions(ndb::Options* options) const {
  auto url = FormatURL("options");
  std::string data;
  NDB_TRY(HTTPGet(url, &data));

  rapidjson::Document d;
  if (d.Parse(data.c_str()).HasParseError()) {
    return Result::Error("Parse JSON failed.");
  }

  if (!d.IsObject() ||
      !d.HasMember("node") ||
      !d["node"].HasMember("value") ||
      !d["node"]["value"].IsString()) {
    return Result::Error("Invalid JSON.");
  }

  rapidjson::Document v;
  if (v.Parse(d["node"]["value"].GetString()).HasParseError()) {
    return Result::Error("Parse JSON failed.");
  }
  if (!v.IsObject()) {
    return Result::Error("Invalid JSON");
  }

  for (auto module = v.MemberBegin(); module != v.MemberEnd(); module++) {
    std::string name(module->name.GetString());
    if (!module->value.IsObject()) {
      return Result::Error("Invalid option %s", name.c_str());
    }
    for (auto it = module->value.MemberBegin(); it != module->value.MemberEnd(); it++) {
      auto k = name + "." + it->name.GetString();
      if (!it->value.IsString()) {
        return Result::Error("Invalid option %s", k.c_str());
      }
      NDB_TRY(options->Set(k, it->value.GetString()));
    }
  }

  return Result::OK();
}

Result Center::GetNamespaces(std::map<std::string, Configs>* nss) const {
  auto url = FormatURL("namespaces");
  std::string data;
  NDB_TRY(HTTPGet(url, &data));

  rapidjson::Document d;
  if (d.Parse(data.c_str()).HasParseError()) {
    return Result::Error("Parse JSON failed.");
  }

  if (!d.IsObject() ||
      !d.HasMember("node") ||
      !d["node"].HasMember("nodes") ||
      !d["node"]["nodes"].IsArray()) {
    return Result::Error("Invalid JSON.");
  }

  const auto& nodes = d["node"]["nodes"];
  for (auto it = nodes.Begin(); it != nodes.End(); it++) {
    if (!it->IsObject() ||
        !it->HasMember("key") ||
        !it->HasMember("value") ||
        !(*it)["key"].IsString() ||
        !(*it)["value"].IsString()) {
      return Result::Error("Invalid JSON.");
    }

    std::string nsname((*it)["key"].GetString());
    nsname = nsname.substr(nsname.rfind('/') + 1, nsname.npos);

    rapidjson::Document ns;
    if (ns.Parse((*it)["value"].GetString()).HasParseError()) {
      return Result::Error("Parse JSON failed.");
    }

    Configs configs;

    if (ns.HasMember("expire")) {
      if (!ns["expire"].IsInt()) {
        return Result::Error("Invalid expire");
      }
      configs.set_expire(ns["expire"].GetInt());
    }

    if (ns.HasMember("maxlen")) {
      if (!ns["maxlen"].IsInt()) {
        return Result::Error("Invalid maxlen");
      }
      configs.set_maxlen(ns["maxlen"].GetInt());
    }

    if (ns.HasMember("pruning")) {
      if (!ns["pruning"].IsString()) {
        return Result::Error("Invalid pruning");
      }
      auto p = ns["pruning"].GetString();
      if (strcasecmp(p, "min") == 0) {
        configs.set_pruning(Pruning::MIN);
      } else if (strcasecmp(p, "max") == 0) {
        configs.set_pruning(Pruning::MAX);
      } else {
        return Result::Error("Invalid pruning: %s", p);
      }
    }

    nss->emplace(nsname, configs);
  }

  return Result::OK();
}

}  // namespace ndb
