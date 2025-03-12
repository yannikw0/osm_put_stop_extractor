"""
Model exported as python.
Name : 20241008_postprocess_put_stops
Group : 
With QGIS : 34004
"""

from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterVectorLayer
from qgis.core import QgsProcessingParameterFeatureSink
from qgis.core import QgsCoordinateReferenceSystem
import processing


class _postprocess_put_stops(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterVectorLayer('put_points_raw', 'put_points_raw', types=[QgsProcessing.TypeVectorPoint], defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSink('Put_stops_postprocessed', 'PuT_stops_postprocessed', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, supportsAppend=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(7, model_feedback)
        results = {}
        outputs = {}

        # Layer reprojizieren UTM 32N
        alg_params = {
            'INPUT': parameters['put_points_raw'],
            'OPERATION': None,
            'TARGET_CRS': QgsCoordinateReferenceSystem('EPSG:25832'),
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['LayerReprojizierenUtm32n'] = processing.run('native:reprojectlayer', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Aggregieren
        alg_params = {
            'AGGREGATES': [{'aggregate': 'concatenate_unique','delimiter': ',','input': 'name','length': 0,'name': 'name','precision': 0,'sub_type': 0,'type': 10,'type_name': 'text'},{'aggregate': 'concatenate_unique','delimiter': ',','input': 'route_type','length': 0,'name': 'route_type','precision': 0,'sub_type': 0,'type': 10,'type_name': 'text'},{'aggregate': 'minimum','delimiter': ',','input': 'service_priority','length': 0,'name': 'service_priority','precision': 0,'sub_type': 0,'type': 2,'type_name': 'integer'},{'aggregate': 'first_value','delimiter': ',','input': 'stoparea_name','length': 0,'name': 'stoparea_name','precision': 0,'sub_type': 0,'type': 10,'type_name': 'text'},{'aggregate': 'first_value','delimiter': ',','input': 'general_type','length': 0,'name': 'general_type','precision': 0,'sub_type': 0,'type': 10,'type_name': 'text'},{'aggregate': 'count','delimiter': ',','input': 'name','length': 0,'name': 'original_feature_count','precision': 0,'sub_type': 0,'type': 2,'type_name': 'integer'}],
            'GROUP_BY': 'Array("stoparea_name", "general_type")',
            'INPUT': outputs['LayerReprojizierenUtm32n']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Aggregieren'] = processing.run('native:aggregate', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # Puffer (lokal verschmelzen)
        alg_params = {
            'DISSOLVE': False,
            'DISTANCE': 500,
            'END_CAP_STYLE': 0,  # Rund
            'INPUT': outputs['Aggregieren']['OUTPUT'],
            'JOIN_STYLE': 0,  # Rund
            'MITER_LIMIT': 2,
            'SEGMENTS': 5,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['PufferLokalVerschmelzen'] = processing.run('native:buffer', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # Zentroide
        alg_params = {
            'ALL_PARTS': True,
            'INPUT': outputs['PufferLokalVerschmelzen']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Zentroide'] = processing.run('native:centroids', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # Feldrechner service_type
        alg_params = {
            'FIELD_LENGTH': 0,
            'FIELD_NAME': 'service_type',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 2,  # Text (string)
            'FORMULA': 'CASE\r\nWHEN "service_priority" = 1 THEN \'high_speed\'\r\nWHEN "service_priority" = 2 THEN \'long_distance\'\r\nWHEN "service_priority" = 3 THEN \'regional\'\r\nWHEN "service_priority" = 4 THEN \'commuter\'\r\nELSE \'\'\r\nEND',
            'INPUT': outputs['Zentroide']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['FeldrechnerService_type'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}

        # Feldrechner Gewicht
        alg_params = {
            'FIELD_LENGTH': 0,
            'FIELD_NAME': 'Gewicht',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 1,  # Ganzzahl (Integer 32 bit)
            'FORMULA': 'CASE\r\n    WHEN "general_type" = \'rail\' AND "service_type" = \'high_speed\' THEN 5\r\n    WHEN "general_type" = \'rail\' AND "service_type" = \'long_distance\' THEN 5\r\n    WHEN "general_type" = \'rail\' AND "service_type" = \'regional\' THEN 4\r\n    WHEN "general_type" = \'rail\' AND "service_type" = \'commuter\' THEN 3\r\n    WHEN "general_type" = \'rail\' AND "service_type" = \'\' THEN 3\r\n    WHEN "general_type" = \'bus\' THEN 1\r\n    ELSE 0\r\nEND',
            'INPUT': outputs['FeldrechnerService_type']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['FeldrechnerGewicht'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}

        # Feldrechner Radius
        alg_params = {
            'FIELD_LENGTH': 0,
            'FIELD_NAME': 'Radius',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 1,  # Ganzzahl (Integer 32 bit)
            'FORMULA': 'CASE\r\nWHEN "Gewicht" = 5 THEN 1000\r\nWHEN "Gewicht" = 4 THEN 500\r\nWHEN "Gewicht" = 3 THEN 250\r\nWHEN "Gewicht" = 2 THEN 175\r\nWHEN "Gewicht" = 1 THEN 100\r\nELSE 0\r\nEND',
            'INPUT': outputs['FeldrechnerGewicht']['OUTPUT'],
            'OUTPUT': parameters['Put_stops_postprocessed']
        }
        outputs['FeldrechnerRadius'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Put_stops_postprocessed'] = outputs['FeldrechnerRadius']['OUTPUT']
        return results

    def name(self):
        return '20241008_postprocess_put_stops'

    def displayName(self):
        return '20241008_postprocess_put_stops'

    def group(self):
        return ''

    def groupId(self):
        return ''

    def createInstance(self):
        return _postprocess_put_stops()
