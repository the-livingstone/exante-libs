import requests
import logging
from bs4 import BeautifulSoup

class Nexus:
    def _get(self, url, stream=False):
        logging.info('Trying to download file {}'.format(url))
        res = requests.get(url, stream=stream, timeout=2)
        if res.status_code != 200:
            raise RuntimeError('Bad http response!\nUrl: {}\nCode: {}\nReason: {}\n{}'.format(url,
                                                                                                res.status_code,
                                                                                                res.reason,
                                                                                                res.text))
        return res

    def get_maven_app(self, project_path, project_name, file_name=None, ver=None):
        """
        download app from nexus maven repo
        :param project_path: project path
        :param project_name: project name
        :param file_name:    filename for local file
        :param ver:  version of the build
        :return: file name of downloaded file
        """
        pattern = 'http://{{host}}/repository/{path}/{name}/{{{{tail}}}}'.format(path=project_path,name=project_name)

        if not ver:
            try:
                base_url = pattern.format(host='nexus.ghcg.com')
                res = self._get(base_url.format(tail='maven-metadata.xml'))
            except:
                base_url = pattern.format(host='nexus3.prod.zorg.sh')
                res = self._get(base_url.format(tail='maven-metadata.xml'))

            ver = BeautifulSoup(res.content, 'lxml').metadata.versioning.release.text

        file_path = '{0}/{1}-{0}-jar-with-dependencies.jar'.format(ver,project_name)
        full_url = base_url.format(tail=file_path)

        res = self._get(full_url,True)

        if not file_name:
            file_name = '{1}-{0}-jar-with-dependencies.jar'.format(ver,project_name)
        with open(file_name, 'wb') as f:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        res.close()

        return file_name
